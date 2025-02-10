select
    concat(r._id, '_', row_number() over (partition by _id order by coalesce(CAST(i.itemNumber AS STRING), i.brandCode, i.barcode, i.partnerItemId) desc)) as receipt_item_id,
    r._id as receipt_id,
    r.userId as user_id,
    i.itemNumber as item_number,
    i.brandCode as brand_code,    
    i.barcode as bar_code,
    i.partnerItemId as partner_item_id,
    i.originalMetaBriteBarcode as original_meta_brite_barcode,
    i.rewardsProductPartnerId as rewards_product_partner_id,    
    i.deleted,
    i.originalReceiptItemText as original_receipt_item_text,
    i.description,

    i.quantityPurchased as quantity_purchased,
    i.itemPrice as item_price,
    i.originalFinalPrice as original_final_price,
    i.priceAfterCoupon as price_after_coupon,    
    i.discountedItemPrice as discounted_item_price,
    i.finalPrice as final_price,
    i.pointsEarned as points_earned,    
  FROM 
    `tli-sample-01.fetch.receipts` r,
    UNNEST(r.rewardsReceiptItemList) as i