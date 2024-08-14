use crate::{
    event::{MarketEvent, MarketIter},
    exchange::ExchangeId,
    subscription::book::{Level, OrderBookL1},
};

use super::message::BybitPayload;
use barter_integration::model::Exchange;
use chrono::Utc;
use serde::{Deserialize, Serialize};

/// Terse type alias for an Bybit real-time OrderBook Level1
/// (top of book) WebSocket message.
pub type BybitOrderBook = BybitPayload<BybitOrderBookInner>;

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BybitOrderBookInner {
    s: String,
    b: Vec<BybitLevel>,
    a: Vec<BybitLevel>,
    u: i64,
    seq: i64,
}

#[derive(Clone, Copy, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BybitLevel {
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub price: f64,
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub amount: f64,
}

impl From<BybitLevel> for Level {
    fn from(level: BybitLevel) -> Self {
        Level {
            price: level.price,
            amount: level.amount,
        }
    }
}

impl<InstrumentId: Clone> From<(ExchangeId, InstrumentId, BybitOrderBook)>
    for MarketIter<InstrumentId, OrderBookL1>
{
    fn from(
        (exchange_id, instrument, mut book): (ExchangeId, InstrumentId, BybitOrderBook),
    ) -> Self {
        [Ok(MarketEvent {
            exchange_time: book.time,
            received_time: Utc::now(),
            exchange: Exchange::from(exchange_id),
            instrument: instrument.clone(),
            kind: OrderBookL1 {
                last_update_time: book.time, //TODO: fix me...
                // TODO: give actual errors...
                best_bid: book.data.b.pop().unwrap().into(),
                best_ask: book.data.a.pop().unwrap().into(),
            },
        })]
        .into_iter()
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
        fn test_okx_message_order_book_l1() {
            struct TestCase {
                input: &'static str,
                expected: Result<BybitOrderBook, SocketError>,
            }

            let tests = vec![TestCase {
                input: r#"
                            {
                                "topic": "orderbook.50.BTCUSDT",
                                "type": "snapshot",
                                "ts": 1672304484978,
                                "data": {
                                    "s": "BTCUSDT",
                                    "b": [
                                        [
                                            "16493.50",
                                            "0.006"
                                        ],
                                        [
                                            "16493.00",
                                            "0.100"
                                        ]
                                    ],
                                    "a": [
                                        [
                                            "16611.00",
                                            "0.029"
                                        ],
                                        [
                                            "16612.00",
                                            "0.213"
                                        ]
                                    ],
                                    "u": 18521288,
                                    "seq": 7961638724    
                                },
                                "cts": 1672304484976
                            }
                        "#,
                expected: Ok(BybitOrderBook {
                    subscription_id: SubscriptionId::from("orderbook.50|BTCUSDT"),
                    time: datetime_utc_from_epoch_duration(Duration::from_millis(1672304484978)),
                    data: BybitOrderBookInner {
                        s: "BTCUSDT".to_string(),
                        b: vec![
                            BybitLevel {
                                price: 16493.5,
                                amount: 0.006,
                            },
                            BybitLevel {
                                price: 16493.0,
                                amount: 0.1,
                            },
                        ],
                        a: vec![
                            BybitLevel {
                                price: 16611.0,
                                amount: 0.029,
                            },
                            BybitLevel {
                                price: 16612.0,
                                amount: 0.213,
                            },
                        ],
                        u: 18521288,
                        seq: 7961638724,
                    },
                    r#type: "snapshot".to_string(),
                }),
            }];

            for (index, test) in tests.into_iter().enumerate() {
                let actual = serde_json::from_str::<BybitOrderBook>(test.input);
                match (actual, test.expected) {
                    (Ok(actual), Ok(expected)) => {
                        assert_eq!(actual, expected, "TC{} failed", index)
                    }
                    (Err(_), Err(_)) => {
                        // Test passed
                    }
                    (actual, expected) => {
                        // Test failed
                        panic!("TC{index} failed because actual != expected. \nActual: {actual:?}\nExpected: {expected:?}\n");
                    }
                }
            }
        }
    }
}
