SELECT `date`,
    act_symbol,
    current_portion_long_term_debt,
    total_current_liabilities,
    long_term_debt,
    total_liabilities
FROM earnings.balance_sheet_liabilities
WHERE period = 'Quarter'