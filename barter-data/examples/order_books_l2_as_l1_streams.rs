use barter_data::{event::MarketEvent, exchange::{binance::futures::BinanceFuturesUsd, bybit::futures::BybitPerpetualsUsd}, streams::Streams, subscription::book::{IntoOrderBookL1, OrderBooksL1, OrderBooksL2}};
use barter_integration::model::instrument::kind::InstrumentKind;
use futures::StreamExt;
use tracing::info;

#[tokio::main]
async fn main() {
    // Initialise INFO Tracing log subscriber
    init_logging();

    let l1_stream = Streams::<OrderBooksL1>::builder()
        .subscribe([
            (BinanceFuturesUsd::default(), "eth", "usdt", InstrumentKind::Perpetual, OrderBooksL1),
        ])
        .init()
        .await
        .unwrap();

    let l2_stream = Streams::<OrderBooksL2>::builder()
        .subscribe([
            (BybitPerpetualsUsd::default(), "eth", "usdt", InstrumentKind::Perpetual, OrderBooksL2),
        ])
        .init()
        .await
        .unwrap();

    // Merge the streams
    let mut streams = futures::stream::select(
        l1_stream.join_map().await,
        l2_stream.join_map().await.map(|(exchange, event)| {
            // Convert L2 to L1 by taking just the best bid/ask
            let l1_event = MarketEvent {
                exchange_time: event.exchange_time,
                received_time: event.received_time,
                exchange: exchange.into(),
                instrument: event.instrument,
                // NOTE: We are using the `IntoOrderBookL1` trait to convert the L2 event to an L1 event.
                //       This has only been tested for orderbook.1 (i.e. depth=1) so far.
                kind: event.kind.into_l1(),
            };
            (exchange, l1_event)
        }),
    );

    while let Some((exchange, order_book_l1)) = streams.next().await {
        info!("Exchange: {exchange}, MarketEvent<OrderBookL1>: {order_book_l1:?}");
    }

}

// Initialise an INFO `Subscriber` for `Tracing` Json logs and install it as the global default.
fn init_logging() {
    tracing_subscriber::fmt()
        // Filter messages based on the INFO
        .with_env_filter(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        // Disable colours on release builds
        .with_ansi(cfg!(debug_assertions))
        // Enable Json formatting
        .json()
        // Install this Tracing subscriber as global default
        .init()
}
