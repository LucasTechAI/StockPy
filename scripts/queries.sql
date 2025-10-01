-- Query to preview the news_raw table
SELECT * FROM "news_db"."news_raw" ORDER BY extracted_at DESC LIMIT 10;
SELECT count(*) FROM "news_db"."news_raw";

-- Query to preview the news_clean table
SELECT * FROM "news_db"."news_clean" ORDER BY extracted_at DESC LIMIT 10;
SELECT count(*) FROM "news_db"."news_clean";


-- Query to preview the stock_prices_best_row table
SELECT * FROM "stock_db"."stock_prices_best_row" ORDER BY dataproc DESC LIMIT 10;
SELECT count(*) FROM "stock_db"."stock_prices_best_row";


-- Query to preview the stocks_clean table
SELECT * FROM "stock_db"."stocks_clean" ORDER BY dataproc DESC LIMIT 10;
SELECT count(*) FROM "stock_db"."stocks_clean";

