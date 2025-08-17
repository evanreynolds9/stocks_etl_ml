SELECT `date`,
    act_symbol,
    sales,
    gross_profit,
    net_income,
    diluted_net_eps
FROM earnings.income_statement
WHERE period = 'Quarter'