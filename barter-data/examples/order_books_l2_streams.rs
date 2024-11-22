use barter_data::{
    exchange::{binance::spot::BinanceSpot, bybit::spot::BybitSpot, ExchangeId},
    streams::Streams,
    subscription::book::OrderBooksL2,
};
use barter_integration::model::instrument::kind::InstrumentKind;
use tracing::info;
use futures::StreamExt;

const USE_BYBIT: bool = true;
const USE_BINANCE: bool = false;

#[rustfmt::skip]
#[tokio::main]
async fn main() {
    // Initialise INFO Tracing log subscriber
    init_logging();

    // Start with empty builder
    let mut builder = Streams::<OrderBooksL2>::builder();

    // Add Bybit subscription if enabled
    // NOTE: Bybit OrderBookL2 is currently set to depth=1 (i.e. orderbook.1).
    //       This can be modified in `barter-data/src/exchange/bybit/channel.rs`.
    if USE_BYBIT {
        builder = builder
            // Separate WebSocket connection for BTC_USDT stream since it's very high volume
            .subscribe([
                (BybitSpot::default(), "btc", "usdt", InstrumentKind::Spot, OrderBooksL2),
            ])

            // Separate WebSocket connection for ETH_USDT stream since it's very high volume
            .subscribe([
                (BybitSpot::default(), "eth", "usdt", InstrumentKind::Spot, OrderBooksL2),
            ])

            // Lower volume Instruments can share a WebSocket connection
            .subscribe([
                (BybitSpot::default(), "xrp", "usdt", InstrumentKind::Spot, OrderBooksL2),
                (BybitSpot::default(), "sol", "usdt", InstrumentKind::Spot, OrderBooksL2),
                (BybitSpot::default(), "avax", "usdt", InstrumentKind::Spot, OrderBooksL2),
                (BybitSpot::default(), "ltc", "usdt", InstrumentKind::Spot, OrderBooksL2),
            ])
    }

    // Add Binance subscription if enabled
    if USE_BINANCE {
        builder = builder
             // Separate WebSocket connection for BTC_USDT stream since it's very high volume
            .subscribe([
                (BinanceSpot::default(), "btc", "usdt", InstrumentKind::Spot, OrderBooksL2),
            ])

            // Separate WebSocket connection for ETH_USDT stream since it's very high volume
            .subscribe([
                (BinanceSpot::default(), "eth", "usdt", InstrumentKind::Spot, OrderBooksL2),
            ])

            // Lower volume Instruments can share a WebSocket connection
            .subscribe([
                (BinanceSpot::default(), "xrp", "usdt", InstrumentKind::Spot, OrderBooksL2),
                (BinanceSpot::default(), "sol", "usdt", InstrumentKind::Spot, OrderBooksL2),
                (BinanceSpot::default(), "avax", "usdt", InstrumentKind::Spot, OrderBooksL2),
                (BinanceSpot::default(), "ltc", "usdt", InstrumentKind::Spot, OrderBooksL2),
            ])
    }

    // Initialize streams
    let mut streams = builder.init().await.unwrap();

    // Join all exchange OrderBooksL2 streams into a single tokio_stream::StreamMap
    let mut joined_stream = streams.join_map().await;

    while let Some((exchange, order_book_l2)) = joined_stream.next().await {
        info!("Exchange: {exchange}, MarketEvent<OrderBookL2>: {order_book_l2:?}");
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
