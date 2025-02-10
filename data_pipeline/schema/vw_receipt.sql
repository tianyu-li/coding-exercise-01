SELECT
    _id as receipt_id,
    userId as user_id,
    cast(createDate as date) as create_date,
    cast(modifyDate as date) as modify_date,
    cast(finishedDate as date) as finished_date,
    cast(purchaseDate as date) as purchase_date,
    cast(pointsAwardedDate as date) as points_awarded_date,
    cast(dateScanned as date) as date_scanned,
    pointsEarned as points_earned,
    bonusPointsEarned as bonus_points_earned,
    bonusPointsEarnedReason as bonus_points_earned_reason,
    rewardsReceiptStatus as rewards_receipt_status, 
    purchasedItemCount as purchased_item_count,
    totalSpent as total_spent
  FROM `tli-sample-01.fetch.receipts`