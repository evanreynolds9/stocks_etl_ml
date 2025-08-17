SELECT `date`,
    act_symbol,
    total_current_assets,
    total_assets
FROM earnings.balance_sheet_assets
WHERE period = 'Quarter'