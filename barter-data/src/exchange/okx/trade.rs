use crate::{
    event::{MarketEvent, MarketIter},
    exchange::ExchangeId,
    subscription::trade::PublicTrade,
};
use barter_integration::model::{Exchange, Side};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// For backwards compatibility
pub use super::message::OkxMessage;

/// Terse type alias for an [`Okx`](super::Okx) real-time trades WebSocket message.
pub type OkxTrades = OkxMessage<OkxTrade>;

/// [`Okx`](super::Okx) real-time trade WebSocket message.
///
/// See [`OkxMessage`] for full raw payload examples.
///
/// See docs: <https://www.okx.com/docs-v5/en/#websocket-api-public-channel-trades-channel>
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct OkxTrade {
    #[serde(rename = "tradeId")]
    pub id: String,
    #[serde(rename = "px", deserialize_with = "barter_integration::de::de_str")]
    pub price: f64,
    #[serde(rename = "sz", deserialize_with = "barter_integration::de::de_str")]
    pub amount: f64,
    pub side: Side,
    #[serde(
        rename = "ts",
        deserialize_with = "barter_integration::de::de_str_u64_epoch_ms_as_datetime_utc"
    )]
    pub time: DateTime<Utc>,
}

impl<InstrumentId: Clone> From<(ExchangeId, InstrumentId, OkxTrades)>
    for MarketIter<InstrumentId, PublicTrade>
{
    fn from((exchange_id, instrument, trades): (ExchangeId, InstrumentId, OkxTrades)) -> Self {
        trades
            .data
            .into_iter()
            .map(|trade| {
                Ok(MarketEvent {
                    exchange_time: trade.time,
                    received_time: Utc::now(),
                    exchange: Exchange::from(exchange_id),
                    instrument: instrument.clone(),
                    kind: PublicTrade {
                        id: trade.id,
                        price: trade.price,
                        amount: trade.amount,
                        side: trade.side,
                    },
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod de {
        use super::*;
        use barter_integration::{
            de::datetime_utc_from_epoch_duration, error::SocketError, model::SubscriptionId,
        };
        use std::time::Duration;

        #[test]
        fn test_okx_message_trades() {
            let input = r#"
            {
                "arg": {
                    "channel": "trades",
                    "instId": "BTC-USDT"
                },
                "data": [
                    {
                        "instId": "BTC-USDT",
                        "tradeId": "130639474",
                        "px": "42219.9",
                        "sz": "0.12060306",
                        "side": "buy",
                        "ts": "1630048897897"
                    }
                ]
            }
            "#;

            let actual = serde_json::from_str::<OkxTrades>(input);
            let expected: Result<OkxTrades, SocketError> = Ok(OkxTrades {
                subscription_id: SubscriptionId::from("trades|BTC-USDT"),
                data: vec![OkxTrade {
                    id: "130639474".to_string(),
                    price: 42219.9,
                    amount: 0.12060306,
                    side: Side::Buy,
                    time: datetime_utc_from_epoch_duration(Duration::from_millis(1630048897897)),
                }],
            });

            match (actual, expected) {
                (Ok(actual), Ok(expected)) => {
                    assert_eq!(actual, expected, "TC failed")
                }
                (Err(_), Err(_)) => {
                    // Test passed
                }
                (actual, expected) => {
                    // Test failed
                    panic!("TC failed because actual != expected. \nActual: {actual:?}\nExpected: {expected:?}\n");
                }
            }
        }
    }
}
