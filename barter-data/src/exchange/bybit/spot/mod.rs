use barter_integration::model::instrument::Instrument;

use super::{book::l2::BybitBookUpdater, Bybit, ExchangeServer};
use crate::{exchange::{ExchangeId, StreamSelector}, subscription::book::{OrderBook, OrderBooksL1, OrderBooksL2}, transformer::book::MultiBookTransformer, ExchangeWsStream};
//use crate::exchange::bybit::book::l1::BybitBookL1Updater;

/// [`BybitSpot`] WebSocket server base url.
///
/// See docs: <https://bybit-exchange.github.io/docs/v5/ws/connect>
pub const WEBSOCKET_BASE_URL_BYBIT_SPOT: &str = "wss://stream.bybit.com/v5/public/spot";

/// [`Bybit`] spot exchange.
pub type BybitSpot = Bybit<BybitServerSpot>;

/// [`Bybit`] spot [`ExchangeServer`].
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct BybitServerSpot;

impl ExchangeServer for BybitServerSpot {
    const ID: ExchangeId = ExchangeId::BybitSpot;

    fn websocket_url() -> &'static str {
        WEBSOCKET_BASE_URL_BYBIT_SPOT
    }
}

//pub struct OrderBooksL1;

// impl SubscriptionKind for OrderBooksL1 {
//     type Event = OrderBook;
// }

// impl StreamSelector<Instrument, OrderBooksL1> for BybitSpot {
//     type Stream = ExchangeWsStream<
//         MultiBookTransformer<Self, Instrument, OrderBook, BybitBookL1Updater>,
//     >;
// }

impl StreamSelector<Instrument, OrderBooksL2> for BybitSpot {
    type Stream = ExchangeWsStream<
        MultiBookTransformer<Self, Instrument, OrderBooksL2, BybitBookUpdater>,
    >;
}
