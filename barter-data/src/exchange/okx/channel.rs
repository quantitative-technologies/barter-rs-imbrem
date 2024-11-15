use super::Okx;
use crate::{
    subscription::{book::OrderBooksL1, trade::PublicTrades, Subscription},
    Identifier,
};
use serde::Serialize;

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Okx`] channel to be subscribed to.
///
/// See docs: <https://www.okx.com/docs-v5/en/#websocket-api-public-channel>
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct OkxChannel(pub &'static str);

impl OkxChannel {
    /// [`Okx`] real-time trades channel.
    ///
    /// See docs: <https://www.okx.com/docs-v5/en/#websocket-api-public-channel-trades-channel>
    pub const TRADES: Self = Self("trades");
    /// [`Okx`] L1 order books channel channel.
    ///
    /// See docs: <https://www.okx.com/docs-v5/en/#websocket-api-public-channel-trades-channel>
    pub const ORDER_BOOK_L1: Self = Self("bbo-tbt");
}

impl<Instrument> Identifier<OkxChannel> for Subscription<Okx, Instrument, PublicTrades> {
    fn id(&self) -> OkxChannel {
        OkxChannel::TRADES
    }
}

impl<Instrument> Identifier<OkxChannel> for Subscription<Okx, Instrument, OrderBooksL1> {
    fn id(&self) -> OkxChannel {
        OkxChannel::ORDER_BOOK_L1
    }
}

impl AsRef<str> for OkxChannel {
    fn as_ref(&self) -> &str {
        self.0
    }
}
