#include "npcTrading/model.hpp"

namespace npcTrading {

// Order implementation
bool Order::is_open() const {
    return status_ == OrderStatus::SUBMITTED ||
           status_ == OrderStatus::ACCEPTED ||
           status_ == OrderStatus::PARTIALLY_FILLED;
}

bool Order::is_closed() const {
    return status_ == OrderStatus::FILLED ||
           status_ == OrderStatus::CANCELED ||
           status_ == OrderStatus::REJECTED ||
           status_ == OrderStatus::EXPIRED;
}

// Position implementation
void Position::apply_fill(const Fill& fill) {
    double fill_qty = fill.quantity().as_double();
    double fill_price = fill.price().as_double();
    
    // Signed quantity for calculation
    double current_signed_qty = quantity_.as_double();
    if (side_ == PositionSide::SHORT) current_signed_qty = -current_signed_qty;
    
    double fill_signed_qty = fill_qty;
    if (fill.side() == OrderSide::SELL) fill_signed_qty = -fill_qty;
    
    double new_signed_qty = current_signed_qty + fill_signed_qty;
    
    // Update Average Entry Price
    // Case 1: Increasing position (same sign) or starting from 0
    if ((current_signed_qty >= 0 && fill_signed_qty > 0) || 
        (current_signed_qty <= 0 && fill_signed_qty < 0)) {
        
        double total_val = (std::abs(current_signed_qty) * entry_price_.as_double()) + 
                           (std::abs(fill_signed_qty) * fill_price);
        double total_qty = std::abs(new_signed_qty);
        
        if (total_qty > 0) {
            entry_price_ = Price(total_val / total_qty);
        }
    }
    // Case 2: Flipping position (sign changed)
    else if ((current_signed_qty > 0 && new_signed_qty < 0) || 
             (current_signed_qty < 0 && new_signed_qty > 0)) {
        entry_price_ = Price(fill_price);
    }
    // Case 3: Reducing position (no sign change, but magnitude decreased)
    // Entry price remains unchanged.
    
    // Update Quantity and Side
    quantity_ = Quantity(std::abs(new_signed_qty));
    if (new_signed_qty > 0) side_ = PositionSide::LONG;
    else if (new_signed_qty < 0) side_ = PositionSide::SHORT;
    else side_ = PositionSide::FLAT;
    
    timestamp_ = fill.timestamp();
}

void Position::update_unrealized_pnl(Price current_price) {
    if (side_ == PositionSide::FLAT) {
        unrealized_pnl_ = Money(0.0, "USD");
        return;
    }
    
    double cp = current_price.as_double();
    double ep = entry_price_.as_double();
    double qty = quantity_.as_double();
    
    double pnl = 0.0;
    if (side_ == PositionSide::LONG) {
        pnl = (cp - ep) * qty;
    } else {
        pnl = (ep - cp) * qty;
    }
    unrealized_pnl_ = Money(pnl, "USD");
}

} // namespace npcTrading
