use crate::{exchange::ExchangeSub, Identifier};
use barter_integration::model::SubscriptionId;
use serde::{Deserialize, Serialize};

/// [`Okx`](super::Okx) market data WebSocket message.
///
/// ### Raw Payload Examples
/// See docs: <https://www.okx.com/docs-v5/en/#websocket-api-public-channel>
/// #### Spot Buy Trade
/// ```json
/// {
///   "arg": {
///     "channel": "trades",
///     "instId": "BTC-USDT"
///   },
///   "data": [
///     {
///       "instId": "BTC-USDT",
///       "tradeId": "130639474",
///       "px": "42219.9",
///       "sz": "0.12060306",
///       "side": "buy",
///       "ts": "1630048897897"
///     }
///   ]
/// }
/// ```
///
/// #### Option Call Sell Trade
/// ```json
/// {
///   "arg": {
///     "channel": "trades",
///     "instId": "BTC-USD-231229-35000-C"
///   },
///   "data": [
///     {
///       "instId": "BTC-USD-231229-35000-C",
///       "tradeId": "4",
///       "px": "0.1525",
///       "sz": "21",
///       "side": "sell",
///       "ts": "1681473269025"
///     }
///   ]
/// }
/// ```
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct OkxMessage<T> {
    #[serde(
        rename = "arg",
        deserialize_with = "de_okx_message_arg_as_subscription_id"
    )]
    pub subscription_id: SubscriptionId,
    pub data: Vec<T>,
}

impl<T> Identifier<Option<SubscriptionId>> for OkxMessage<T> {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

/// Deserialize an [`OkxMessage`] "arg" field as a Barter [`SubscriptionId`].
fn de_okx_message_arg_as_subscription_id<'de, D>(
    deserializer: D,
) -> Result<SubscriptionId, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Arg<'a> {
        channel: &'a str,
        inst_id: &'a str,
    }

    Deserialize::deserialize(deserializer)
        .map(|arg: Arg<'_>| ExchangeSub::from((arg.channel, arg.inst_id)).id())
}
