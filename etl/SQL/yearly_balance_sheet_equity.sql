SELECT `date`,
    act_symbol,
    total_equity,
    book_value_per_share
FROM earnings.balance_sheet_equity
WHERE period = 'Year'