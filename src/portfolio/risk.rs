use crate::portfolio::order::{OrderType, OrderEvent};
use crate::portfolio::error::PortfolioError;

/// Evaluates the risk associated with an OrderEvent to determine if it should be actioned. It can
/// also amend the order (eg/ OrderType) to better fit the risk strategy required for profitability.
pub trait OrderEvaluator {
    /// Default OrderType
    const DEFAULT_ORDER_TYPE: OrderType;

    /// May return an amended OrderEvent if the associated risk is appropriate. Returns None if the
    /// risk is too high.
    fn evaluate_order(&self, order: OrderEvent) -> Result<Option<OrderEvent>, PortfolioError>;
}

/// Default risk manager that implements OrderEvaluator.
#[derive(Debug, Deserialize)]
pub struct DefaultRisk {}

impl OrderEvaluator for DefaultRisk {
    const DEFAULT_ORDER_TYPE: OrderType = OrderType::Market;

    fn evaluate_order(&self, mut order: OrderEvent) -> Result<Option<OrderEvent>, PortfolioError> {
        if self.risk_too_high(&order) {
            return Ok(None);
        }
        order.order_type = DefaultRisk::DEFAULT_ORDER_TYPE;
        Ok(Some(order))
    }
}

impl DefaultRisk {
    fn risk_too_high(&self, _: &OrderEvent) -> bool {
        false
    }
}