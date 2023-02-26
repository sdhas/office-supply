@echo off
set da=%date:~0,2%
set mo=%date:~3,2%
set ye=%date:~6%
@echo on
py office_supply_google_price_crawling_db_V2_1.py %mo%.%da%.%ye%