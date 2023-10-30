# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).


## [1.41.3] - 2023-10-30
- feature/tax-report [abdur.rasyid@majoo.id]
### Added
- Models
    - TaxReport
        - Spark/rawKafka.py ``add dml for etl stream``
### Changes
- Controller
    - TaxReport.py `` add fuction to handle enpoint stream `` 
- Models
    - TaxReport
        - Spark/services.py ``handle rawKafka``
    - TaxReportMart.py `` add logic function for etl stream ``
- main.py `` add endpoint etl stream ``

## [1.41.2] - 2023-10-27
- bugfix/fix-parameter-akunting-kafka-typo [akbar.noto@majoo.id]
### Changes
- Controller
    - Akunting.py `` Fix parameter typo ``
- Models
    - AkuntingMart.py `` Made ids unique before fetching di source ``

## [1.41.1] - 2023-10-25
- fix/etl-product-mart [muhammad.muzakki@majoo.id]
### Changes
- Models
    - ProductV2.py `` add handler if the mart_param is empty ``

## [1.41.0] - 2023-10-24
- feature/busiest-item-time-streaming [muhammad.muzakki@majoo.id]
- feature/recovery-crm [akbar.noto@majoo.id]
- feature/sales_dashboard [abdur.rasyid@majoo.id]
### Added
- Controller/CRM_TransactionManagementUS2.py `` Endpoint handler ``
- Models
    - CRM_TransactionManagementUS2.py `` Logic implementation for recovery ``
    - Dashboard/Spark/rawKafka.py ``add dml for etl stream``
### Changed
- main.py ``add raw and mart endpoint for item time etl, Endpoints for CRM, add endpoint api for etl stream``
- Controllers
    - BusiestItemTime.py ``add raw and mart controller for item time etl``
    - SalesDashboard.ps ``add config etl stream``
- Models
    - BusiestItemTime/BusiestItemTimeMart.py ``add split calculation function for raw and mart``
    - Dashboard
        - Spark/daily.py ``enhance dml laba kotor``
        - Spark/houlry.py ``enhance dml laba kotor``
        - Spark/raw.py ``enhance dml laba kotor``
        - Spark/services.py ``handle rawKafka``
    - SalesDashboardMart.py ``add config to handle etl stream``
- Utility/sql.py `` Add many execution feature``

## [1.40.5] - 2023-10-23
- feature/etl-recovery-promo-batch-update [wikan.kuncara@majoo.id]
### Changed
- Models
    - IntegrityPajakMart.py ``feature: update table Transactions updatedate``
    - RecoveryPromoMart.py ``feature: etl recovery promo batch update``
- Utility/sql.py ``feature: etl recovery promo batch update``

## [1.40.4] - 2023-10-20
- feature/etl-compliment-prod [abdur.rasyid@majoo.id]
### Changed
- Models
    - ComplimentMart.py ``Add filter > 1970``

## [1.40.3] - 2023-10-20
- bugfix/ledger-recovery-backslash [akbar.noto@majoo.id]
### Changed
- Models
    - AkuntingMart.py ``Handle backslash that makes query error``

## [1.40.2] - 2023-10-19
- hotfix/fix-sEnd-sStart [vera@majoo.id]
### Changed
- Models
    - MerchantTransactionSubscriptionMart.py
    ``fix sTimeEnd > sTimeStart, and change query crm - solve outlet name & payment method``

## [1.40.1] - 2023-10-19
- feature/sales_dashboard [abdur.rasyid@majoo.id]
- bugfix/ledger-recovery-quotes [akbar.noto@majoo.id]
### Changed
- Models
	- AkuntingMart.py `` Keep any special characters including quotes  ``
    - dashboard
        - Spark
            - daily.py ``fix: add option custom schema``
            - hourly.py ``fix: add option custom schema``
            - monthly.py ``fix: add option custom schema``
            - yearly.py ``fix: add option custom schema``

## [1.40.0] - 2023-10-18
- feature/add-required-accounting-ledger-cols [akbar.noto@majoo.id]
- feature/etl-outlet-kafka [wikan.kuncara@majoo.id]
### Changed
- main.py `` Add endpoint for ledger recovery, feature: etl outlet kafka ``
- Controller/Akunting.py `` endpoin handler ``
- Controller/OutletSalesV2.py ``feature: etl outlet kafka``
- Models/AkuntingMart.py `` Logic for recovery, ETL adjustment ``
- Modles/OutletSalesMartV2.py ``feature: etl outlet kafka``

## [1.39.1] - 2023-10-18
- fix/recovery-product-duplicate [wikan.kuncara@majoo.id]
### Changed
- Models
    - ProductMartV2.py ``fix: recovery product duplicate in of month``

## [1.39.0] - 2023-10-16
- feature/hutang-v11 [akbar.noto@majoo.id]
- feature/tax-report [abdur.rasyid@majoo.id]
- feature/recovery-pajak-update-transactions [wikan.kuncara@majoo.id]
- fix/integrity-pajak-query-next [wikan.kuncara@majoo.id]
### Added
- Controller
    - TaxReport.py `` Handle endpoint to controller and pass any query param to models ``
- Models
    - AkuntingHutangPiutang/v1_1
        - raw.py `` Logic for raw: m_biaya akunting, rest from klopos ``
        - daily.py `` Logic for daily ``
        - monthly.py `` Logic for monthly ``
        - yearly.py `` Logic for yearly ``
    - TaxReport
        - Spark
            - daily.py `` query & logic calculated daily ``
            - monthly.py `` query & logic calculated montly ``
            - raw.py `` query & logic etl raw data ``
            - service.py `` handle all class on dir pandas ``
            - yearly.py `` query & logic calculated yearly ``
### Changed
- Controller/AkuntingHutangPiutang.py ``Add endpoint handler v1.1``
- Models/AkuntingHutangPiutang/services.py `` Adjust to accommodate hutang v1_1 on top of v1 ``
- Models/AkuntingHutangPiutangMart.py `` Adjust logic to accommodate hutang v1_1 on top of v1 ``
- Models/RecoveryPromoMart.py ``feature: add update table Transactions, feature: add benchmark time update transactions``
- Models/IntegrityPajakMart.py ``fix: integrity pajak query next wrong variable name``
- main.py `` Add API endpoint etl tax``

## [1.38.1] - 2023-10-13
- feature/integrity-pajak-parallel [wikan.kuncara@majoo.id]
### Changed
- Controller
    - IntegrityPajak.py ``feature: integrity pajak parallel by id``
- Models
    - IntegrityPajakMart.py ``feature: integrity pajak parallel by id``

## [1.38.0] - 2023-10-12
- feature/add-akunting-kafka [akbar.noto@majoo.id]
- bugfix/handle-jd-harddelete [akbar.noto@majoo.id]
- fix/recovery-promo-when-finished [wikan.kuncara@majoo.id]
- feature/improvement-recovery-product-monthly [wikan.kuncara@majoo.id]
- Ahmad-Irsyadur-Roziqin/jenkinsfile-edited-online-with-bitbucket-1697109327352 [ahmad.roziqin@majoo.id]
### Changed
- main.py `` add v2 kafka endpoint, remove obsolete api, tidy up non-etl apis ``
- Utility/kafka.py `` Add more flexibility to our kafka message fetcher ``
- Controller
    - Health.py `` Add timezone check ``
    - Akunting.py `` Add v2 kafka endpoint handler ``
- Models
    - Akunting/RDD.py `` remove un-used function ``
    - AkuntingMart.py `` Add logic for kafka etl + custom time to calculate monthly yearly, Re-select & delete based on m_user,jur_no. Remove unnecessary refetch on recovery ``
    - IntegrityPajakMart.py ``fix: recovery promo condition when finished catchup``
    - ProductMartV2.py ``feature: improvement recovery product monthly``
- Jenkinsfile ``remove build in vm klopos prod & akunting staging``

## [1.37.1] - 2023-10-10
- Ahmad-Irsyadur-Roziqin/jenkinsfile-edited-online-with-bitbucket-1696932663961 [ahmad.roziqin@majoo.id]
### Changed
- Jenkinsfile ``tambah etl klopos new 1,2,3``

## [1.37.0] - 2023-10-06
- feature/integrity-pajak [wikan.kuncara@majoo.id]
- hotfix/fix-merchant-transaction [vera@majoo.id]
- coldfix/change-promo-column-product [wikan.kuncara@majoo.id]
### Changed
- main.py ``feature: integrity pajak check``
- Controller
    - IntegrityPajak.py ``feature: integrity pajak check``
- Models
    - IntegrityPajakMart.py ``feature: integrity pajak check``
    - MerchantTransactionSubscriptionMart.py ``null value for date = 0000-00-00 00:00:00``
    - ProductV2/Query.py ``fix: change column promo name etl product v2``

## [1.36.0] - 2023-10-05
- feature/add-promo-stream [akbar.noto@majoo.id]
### Changed
- main.py `` Add endpoint for kafka stream & deactivate some obsolete features ``
- Controller/SalesPromoCouponPoin.py ``Add endpoint handler``
- Models
    - Promo/*.py `` Move coupon & promo logic to promo folders, add some logic for recalculate on hard-delete ``
    - PromoCouponPoinMart.py `` Add implementation for spark by stream , point logic to 'Promo' folder ``

## [1.35.3] - 2023-10-04
- fix/spark-spec-helper [wikan.kuncara@majoo.id]
### Changed
- helper.py ``fix: spark spec helper file``

## [1.35.2] - 2023-10-03
- hotfix/akunting-oom [akbar.noto@majoo.id]
### Changed
- Models/AkuntingMart.py ``Fetch only active_status != 1 for deletion instead of all trx``

## [1.35.1] - 2023-10-02
- feature/spark-spec-env [wikan.kuncara@majoo.id]
### Changed
- helper.py ``fix: add get env from os``

## [1.35.0] - 2023-09-29
- feature/spark-spec-env [wikan.kuncara@majoo.id]
- fix/dispute-delete-and-insert [muhammad.muzakki@majoo.id]
- feature/etl-merchant-transaction-add-paid-at [vera@majoo.id]
- feature/sales_dashboard [abdur.rasyid@majoo.id]
### Changed
- Models
    - BusiestItemTimeMart.py
    - OrderTypeMart.py
    - PaymentTypeMart.py
    ``fix dispute of insert and delete params``
    - MerchantTransactionSubscriptionMart.py `` change query extract from crm ``
    - SalesDashboardMart.py `` adjust n_size_delete for cleansing mart from previous raw ``

## [1.34.2] - 2023-09-26
- feature/etl-busiest-item-time-streaming-2 [muhammad.muzakki@majoo.id]
- feature/recovery-promo-klopos-v2 [muhammad.muzakki@majoo.id]
- fix/product-mart-insert-from-rawmart [wikan.kuncara@majoo.id]
### Changed
- Models
    - BusiestItemTimeMart.py ``fix history default value``
    - ProductMartV2.py ``fix: product mart insert from rawmart``
    - RecoveryPromoKlopos.py ``add calculation for subtotal`

## [1.34.1] - 2023-09-25
- feature/etl-product-streaming [muhammad.muzakki@majoo.id]
### Changed
- Models
    - productMartV2.py ``add dynamic chunk size on params``

## [1.34.0] - 2023-09-25
- feature/sales_dashboard [abdur.rasyid@majoo.id]
- fix/chuck-size-insert-product-kafka [wikan.kuncara@majoo.id]
### Added
- Controller
    - SalesDashboard.py `` Handle endpoint to controller and pass any query param to models ``
- Models
    - dashboard
        - Pandas
            - daily.py `` query & logic calculated daily ``
            - hourly.py `` query & logic calculated hourly ``
            - monthly.py `` query & logic calculated montly ``
            - raw.py `` query & logic etl raw data ``
            - service.py `` handle all class on dir pandas ``
            - yearly.py `` query & logic calculated yearly ``
        - Spark
            - daily.py `` query & logic calculated daily ``
            - hourly.py `` query & logic calculated hourly ``
            - monthly.py `` query & logic calculated montly ``
            - raw.py `` query & logic etl raw data ``
            - service.py `` handle all class on dir pandas ``
            - yearly.py `` query & logic calculated yearly ``
    - SalesDashboardMart.py `` Organize logic function ETL ``
    - ProductMartV2.py ``fix: chuck size insert product kafka``
### Changed
- main.py `` Add API endpoint etl ``

## [1.33.4] - 2023-09-22
- bugfix/recovery-on-extras [akbar.noto@majoo.id]
### Changed
- Models/ExtraMart.py `` Clean all data between range on a user instead by chunk to handle hard delete ``
- main.py `` Take out ETL using pandas ``

## [1.33.3] - 2023-09-21
- fix/outlet-mart-cleansing [wikan.kuncara@majoo.id]
- feature/etl-product-streaming [muhammad.muzakki@majoo.id]
- bugfix/daily-selisih-transactiontype [akbar.noto@majoo.id]
### Changed
- Models
    - OutletSalesMartV2.py
    - productv2.py ``fix typo - history table name``
    - Daily/Spark/raw.py ``Read dari raw untuk hapus mart``
    - DailyReportMart.py ``Delete all between range on recovery``
- main.py `` Take out ETL using pandas ``

## [1.33.2] - 2023-09-20
- feature/etl-product-streaming [muhammad.muzakki@majoo.id]
- Ahmad-Irsyadur-Roziqin/jenkinsfile-edited-online-with-bitbucket-1695183750762 [ahmad.roziqin@majoo.id]
- fix/mart-product-hourly-dispute [wikan.kuncara@majoo.id]
### Changed
- Controller
    - productv2.py ``remove default specs``
- Models
    - ProductMartV2.py ``fix: remove mart product hourly filter status = 1``
- Jenkinsfile ``change IP for vm etl produk``

## [1.33.1] - 2023-09-18
- Ahmad-Irsyadur-Roziqin/jenkinsfile-edited-online-with-bitbucket-1695138474311 [ahmad.roziqin@majoo.id]
### Changed
- Jenkinsfile ``add code for build vm etl produk``

## [1.33.0] - 2023-09-18
- feature/akunting-v2 [akbar.noto@majoo.id]
- feature/etl-product-streaming [muhammad.muzakki@majoo.id]
### Changed
- main.py `` Add endpoint for version 2 ``
- Controller
    - Akunting.py `` Handle endpoint to controller and pass any query param to models ``
- Models
    - AkuntingMart.py `` Add logic for version 2 ``
    - productv2.py ``add function for mart and raw calculation only, distinct process when generate mart params, and dynamic resource configs``
    - OrderType/OrderTypeMart.py ``add new handler that handles ETL errors so kafka will not commit messages``
    - Payment/PaymentMart.py ``add new handler that handles ETL errors so kafka will not commit messages``
### Added
- Models
    - Akunting/v2/*.py `` Adjusting table & logic for version 2 for Saldo & Cashflow ``

## [1.32.2] - 2023-09-18
- feature/etl-compliment-prod [abdur.rasyid@majoo.id]
### Changed
- Models
    - ComplimentMart.py ``change delete methode``
    - RecoveryPromoMart.py ``change function recovery compliment``

## [1.32.1] - 2023-09-15
- feature/etl-compliment-prod [abdur.rasyid@majoo.id]
### Changed
- Models
    - ComplimentMart.py ``add function recovery promo calculated``
    - RecoveryPromoMart.py ``change function recovery compliment``

## [1.32.0] - 2023-09-14
- feature/spec-param-recovery-promo [wikan.kuncara@majoo.id]
### Changed
- Controller
    - RecoveryPromo.py ``feature: spec param recovery promo``
- Models
    - ComplimentMart.py ``feature: spec param recovery promo``
    - ProductMartV2.py ``feature: spec param recovery promo``
    - RecoveryPromoMart.py ``feature: spec param recovery promo``

## [1.31.6] - 2023-09-13
- fix/recovery-promo-sequence [wikan.kuncara@majoo.id]
### Changed
- Controller
    - RecoveryPromo.py ``fix: add table history for running log``
- Models
    - RecoveryPromoMart.py ``fix: add table history for running log``

## [1.31.5] - 2023-09-12
- feature/etl-payment-type-streaming [muhammad.muzakki@majoo.id]
### Changed
- Controller
    - PaymentType.py ``add controller for streaming request``
- Models
    - OrderType/OrderTypeMart.py
    - PaymentType/PaymentTypeMart.py
    ``add calculation for raw and mart only``
- Utility
    - sql.py
    ``add new method for store the streaming calculation history``
- helper.py
    ``increase kafka execution threshold``
- main.py ``add raw and mart calculation only endpoint for payment type etl``

## [1.31.4] - 2023-09-11
- fix/etl-product-streaming [muhammad.muzakki@majoo.id]
- fix/etl-busiest-item-time-streaming [muhammad.muzakki@majoo.id]
- fix/recovery-promo-filter-zero [wikan.kuncara@majoo.id]
### Changed
- Controller
    - BusiestItemTime.py ``add controller for streaming request``
- Models
    - BusiestItemTime/BusiestItemTimeMart.py ``add streaming calculation``
    - ProductV2Mart.py
    - OrderType/OrderTypeMart.py
    - PaymentType/PaymentTypeMart.py
    ``improve error handling so that it is still caught by APM``
    - RecoveryPromoMart.py ``fix: recovery promo filter zero``
- Utility
    - Kafka.py
    ``update error handler``
- main.py ``add streaming endpoint for busiest item time etl``

## [1.31.3] - 2023-09-10
- fix/etl-product-streaming [muhammad.muzakki@majoo.id]
### Changed
- Models
    - ProductV2Mart.py
    - OrderType/OrderTypeMart.py
    - PaymentType/PaymentTypeMart.py
    ``increase max execution threshold from 5 minutes to 20 minutes``

## [1.31.2] - 2023-09-10
- fix/etl-product-streaming [muhammad.muzakki@majoo.id]
### Changed
- Models
    - ProductV2Mart.py ``add zero message handler``
    - OrderType/OrderTypeMart.py
    ``add error handler for closing the kafka connection``

## [1.31.1] - 2023-09-08
- feature/etl-compliment-prod [abdur.rasyid@majoo.id]
- coldfix/promo-calculated-value [akbar.noto@majoo.id]
### Changed
- Models
    - ComplimentMart.py ``enhance promo value to promo calculated value``
    - PromoCouponPointMart.py `` Change column to calculate promo & use nota cabang to fetch parent``

## [1.31.0] - 2023-09-08
- feature/etl-product-streaming [muhammad.muzakki@majoo.id]
### Changed
- Controller
    - ProductV2.py ``add streaming controller``
- main.py ``add streaming endpoint for productv2 etl``
- Models
    - ProductV2Mart.py ``add streaming calculation``

## [1.30.1] - 2023-09-08
- fix/etl-product-monthly-batch-size [wikan.kuncara@majoo.id]
### Changed
- Models
    - ProductMartV2.py ``feature: product monthly and yearly mart filter, feature: benchmark time product v2``

## [1.30.0] - 2023-09-07
- feature/monthly-yearly-filter-product [wikan.kuncara@majoo.id]
- fix/etl-merchant-transaction-duplicate-data [vera@majoo.id]
### Changed
- Models
    - MerchantTransactionSubscriptionMart.py ``change flow, only get data langganan based on source, fix duplicate``
    - ProductMartV2.py ``feature: product monthly and yearly mart filter, feature: benchmark time product v2``

## [1.29.1] - 2023-09-07
- hotfix/fix-chunk-promo [akbar.noto@majoo.id]
### Changed
- Models
    - CouponMart.py ``fix chunk salah parameter ``

## [1.29.0] - 2023-09-06
- feature/etl-payment-type [muhammad.muzakki@majoo.id]
- feature/etl-recovery-promo [wikan.kuncara@majoo.id]
- fix/streaming-etl [muhammad.muzakki@majoo.id]
### Changed
- Controller
    - OrderType.py
    - PaymentType.py ``add streaming controller, add timeout in request param``
- main.py ``add streaming endpoint for payment type etl, feature: etl recovery promo``
- Models
    - PaymentType/OrderTypeMart.py ``add dynamic timeout`
    - PaymentType/PaymentTypeMart.py ``add streaming calculation, add dynamic timeout``
    - PaymentType/PaymentTypeFunctions.py ``fix recovery code``
- Utility
    - sql.py ``feature: etl recovery promo``
    - Kafka.py ``add null value handler and dynamic timeout``
### Added
- Controller
    - RecoveryPromo.py ``feature: etl recovery promo``
- Models
    - \__init__.py ``feature: etl recovery promo``
    - RecoveryPromoMart.py ``feature: etl recovery promo``

## [1.28.0] - 2023-09-05
- feature/etl-order-type-streaming [muhammad.muzakki@majoo.id]
- feature/etl-payment-type [muhammad.muzakki@majoo.id]
- feature/chunksize-on-promo-cashier [akbar.noto@majoo.id]
- feature/recovery-promo-klopos [muhammad.muzakki@majoo.id]
- coldfix/precision-limit-on-daily [akbar.noto@majoo.id]
### Changed
- Controller
    - OrderType.py ``add streaming controller``
    - SalesEachCashier.py `` add chunk size parameter ``
    - SalesPromoCouponPoin.py `` add chunk size && spark spec parameter``
- helper.py ``add load kafka configuration``
- main.py ``add streaming endpoint for order type etl and payment type``
- requirement.txt ``add installation for confluent-kafka``
- Models
    - EachCashierMart.py `` Implement chunk size through parameter, default 500 if not defined ``
    - PromoCouponPoinMart.py `` Implement chunk size through parameter, default 500 if not defined && custom spec on recovery ``
    - PromoMart.py `` Implement chunk size on feature promo ``
    - CouponMart.py `` Implement chunk size on feature coupon ``
    - OrderTypeMart ``add streaming calculation``
    - Daily/Spark/*.py `` Add custom schema for numeric data type on raw tables bcs of precision limit on spark ``
    - RecoveryPromoKlopos
        - RecoveryPromoKlopos.py ``fix wrong query for sales_order_items_sales_order_item_id``
### Added
- Controler
    - PaymentType.py ``new report``
- Models
    - PaymentTypeMart.py ``new report``
    - PaymentTypeFunctions.py ``new report``
- Utility
    - Kafka.py ``function for utilize kafka topic``

## [1.27.0] - 2023-09-04
- feature/hutang-piutang [akbar.noto@majoo.id]
- coldfix/fix-recovery-queries-on-dailyreport [akbar.noto@majoo.id]
- feature/recovery-promo-klopos [muhammad.muzakki@majoo.id]
### Changed
- Controller
    - RecoveryPromoKlopos.py
- main.py ``apply hutang piutang endpoints ``
- Models
    - DailyReportMart.py `` Add "AND" on the query to fix query clean & transform``
    - Daily/Spark/*.py `` Add custom schema for numeric data type bcs of precision limit on spark ``
    - RecoveryPromoKlopos/RecoveryPromoKlopos.py
- Utility.py
``add recovery sales order (promo klopos)``
### Added
- Controler/AkuntingHutangPiutang.py `` Add controller & apis endpoints ``
- Models
    - AkuntingHutangPiutang.py `` logic implementation hutang piutang ``
    - AkuntingHutangPiutang/*.py `` logic hutang piutang etl ``

## [1.26.1] - 2023-08-31
- fix/timezone-query-etl [wikan.kuncara@majoo.id]
- fix/product-recovery-id-outlet [wikan.kuncara@majoo.id]
### Changed
- Controller
    - ProductV2.py ``change default delay 5s``
- Models
    - BusiestTimeMart.py ``fix: timezone query etl``
    - OutletSalesMartV2.py ``fix: timezone query etl``
    - ProductMartV2.py ``fix: timezone query etl, fix: id outlet query in etl product recovery, change delay deafult 5s``

## [1.26.0] - 2023-08-23
- feature/etl-merchant-transaction-subscription [vera@majoo.id]
- feature/version-endpoint [wikan.kuncara@majoo.id]
- fix/hpp-addon-outlet [wikan.kuncara@majoo.id]
### Added
- Controller
    - MerchantTransactionSubscripton.py
- Models
    - MerchantTransactionSubscriptionMart.py
    ``add pipeline for etl mart_merchant_transaction``
### Changed
- Controller
    - Health.py ``feature: version endpoint and add version in health check endpoint``
- Models
    - OutletSalesMartV2.py ``fix: reversed operator in query hpp addon etl outlet``
- main.py
  ``add new endpoint for etl merchant transaction subscription, add version in health check endpoint``
- helper.py
  ``add new function to get connection for CRM Delivery``

## [1.25.12] - 2023-08-21
- feature/dynamic-delay-product-outlet [wikan.kuncara@majoo.id]
### Changed
- Controllers
    - OutletSalesV2.py ``feature: dynamic delay etl product v2 dan outlet v2``
    - ProductV2.py ``feature: dynamic delay etl product v2 dan outlet v2``
- Models
    - OutletSalesMartV2.py ``feature: dynamic delay etl product v2 dan outlet v2``
    - ProductMartV2.py ``feature: dynamic delay etl product v2 dan outlet v2``

## [1.25.11] - 2023-08-18
- hotfix/dates-recovery-filters [akbar.noto@majoo.id]
- git checkout Ahmad-Irsyadur-Roziqin/jenkinsfile-edited-online-with-bitbucket-1692590562704 [ahmad.roziqin@majoo.id]
### Changed
- Models `` change to gte (greater than equal) instead of gt on recovery ``
    - AkuntingMart.py
    - Akunting/Pandas/RawService.py
    - BusiestItemTime/BusiestItemTimeMart.py
    - BusiestTimeMart.py
    - OrderType/OrderTypeMart.py
- Jenkinsfile ``remove code for build vm recovery``

## [1.25.10] - 2023-08-15
- coldfix/akunting-remove-join-cabang [akbar.noto@majoo.id]
- coldfix/daily-change-ordertype2transactiontype [akbar.noto@majoo.id]
### Changed
- main.py `` comment live recovery so it doesn't accidentally called ``
- Models
    - AkuntingMart.py `` remove join cabang ``
    - Akunting/Pandas/RawService.py `` remove join cabang ``
    - Daily/*/raw.py ``change order type to transaction type``

## [1.25.9] - 2023-08-11
- coldfix/handle-null-value-outlet [wikan.kuncara@majoo.id]
### Changed
- Models
    - OutletSalesMartV2.py ``fix: handle null value and change type casting``

## [1.25.8] - 2023-08-10
- feature/etl-utilisasi [abdur.rasyid@majoo.id]
- coldfix/laba-kotor-outlet [wikan.kuncara@majoo.id]
### Changed
- Models
    - OutletSalesMartV2.py ``fix: laba kotor etl outlet``
    - UtilizationMart.py `` add summary raw reservation ``

## [1.25.7] - 2023-08-09
- coldfix/add-hourly-on-dailyreport [akbar.noto@majoo.id]
### Added
- Models/Daily/*/hourly.py `` logic for hourly ``
### Changed
- Models
    - DailyReportMart.py `` add implementation for hourly ``
    - Daily/Pandas
        - daily.py `` fix bug when there is only refund trx on daily ``
        - raw.py `` create filter for hourly ``
        - services.py `` hourly alias for implementation ``
    - Daily/Spark/*.py `` create filter for hourly & class alias ``

## [1.25.6] - 2023-08-07
- enhancement/etl-product-dynamic-backdate [muhammad.muzakki@majoo.id]
### Changed
- Controller
    - Product.py ``added delay on parameter``
- Models
    - ProductMart.py ``added dynamic delay``

## [1.25.5] - 2023-08-03
- feature/etl-reservation-prod [abdur.rasyid@majoo.id]
### Changed
- Models
    - ReservationMart.py ``change dataframe name``

## [1.25.4] - 2023-08-02
- Yogie-Prasetya/jenkinsfile-edited-online-with-bitbucket-1690968839539 [ahmad.roziqin@majoo.id]
### Changed
- Jenkinsfile ``add new vm for recovery activity``

## [1.25.3] - 2023-08-01
- coldfix/akunting-catch-cabang [akbar.noto@majoo.id]
- coldfix/endtime-duplicate-error [wikan.kuncara@majoo.id]
- feature/utilisasi [abdur.rasyid@majoo.id]
### Changed
- main.py ``add new endpoint for live recovery``
- Controller
    - Akunting.py ``add new endpoint handler``
- Models
    - AkuntingMart.py ``add new logic for live producitons recoveries, define if named-rdd is live recovery/recovery/standart etl``
    - Akunting/*.py `` add implementation of 3 types named-rdd``
    - BusiestTimeMart.py ``fix: duplicate error because endtime > now``
    - OutletSalesMartV2.py ``fix: duplicate error because endtime > now``
    - OutletSalesMartV2.py ``fix: duplicate error because endtime > now``
    - UtilizationMart.py `` change df_reserve to df_utilization``

## [1.25.2] - 2023-07-31
- fix/recovery-etl-product [muhammad.muzakki@majoo.id]
- Ahmad-Irsyadur-Roziqin/jenkinsfile-edited-online-with-bitbucket-1690518954162 [ahmad.roziqin@majoo.id]
### Changed
- Models
    - Product.py ``fixed ambiguous error when retrieving data from source``
- Jenkinsfile ``ubah host ip pub ke private, karna jenkin pindah ke gcp``

## [1.25.1] - 2023-07-27
- coldfix/fix-recovery-akunting [akbar.noto@majoo.id]
- coldfix/recovery-outlet-product [wikan.kuncara@majoo.id]
### Changed
- main.py ``Change debounce duration``
- Models
    - AkuntingMart.py ``Delete raw and mart on certain dates of a user, then recalculate all data from source``
    - OutletSalesMartV2.py ``fix: outlet recovery transaction_refund_reason not found``
    - ProductMartV2.py ``fix: product recovery ambiguous m_cabang_id_cabang``

## [1.25.0] - 2023-07-26
- feature/generalize-alert-recovery [akbar.noto@majoo.id]
### Changed
- main.py ``Add log specific for recovery, change notification to v2 (more generalized both etl and recovery)``
- notification.py ``Add alert functionality, only send two types : etl and recovery ``

## [1.24.5] - 2023-07-25
- coldfix/transaction-type-laporan-order [wikan.kuncara@majoo.id]
### Changed
- Models
    - OutletSalesMartV2.py
    ``fix: null handling transaction type``

## [1.24.4] - 2023-07-24
- coldfix/refund-condition [muhammad.muzakki@majoo.id]
### Changed
- Models
    - BusiestItemTime/BusiestItemTimeMart.py
    - OrderType/OrderTypeMart.py
    - ProductMart.py
    ``fixing wrong conditions to get non-refund data``

## [1.24.3] - 2023-07-21
- coldfix/refund-condition-outlet [wikan.kuncara@majoo.id]
- coldfix/refund-addon-addondetail-extras-promo [akbar.noto@majoo.id]
- coldfix/refund-daily-cashier  [akbar.noto@majoo.id]
### Changed
- Models
    - AddonDetailMart.py `fix extract non-refund`
    - AddonMart.py `fix extract non-refund`
    - Daily `Adjustment new col to indicate whether the trx is refund or not`
        - */raw.py `Insert new col is_refund`
        - */daily.py `Computation adjustment`
    - EachCashierMart.py `Adjustment new col to indicate whether the trx is refund or not`
    - Extras/PandasService.py `fix extract non-refund for extras pandas`
    - Extras/Raw.py `fix extract non-refund for spark`
    - OutletSalesMartV2.py
    ``fix: excess parentheses``
    - PromoCouponPointMart.py `fix extract non-refund raw`

## [1.24.2] - 2023-07-20
- coldfix/refund-condition [muhammad.muzakki@majoo.id]
- coldfix/refund-condition-product [wikan.kuncara@majoo.id]
- coldfix/refund-condition-outlet [wikan.kuncara@majoo.id]
### Changed
- Models
    - BusiestItemTime/BusiestItemTimeMart.py
    - OrderType/OrderTypeMart.py
    - OutletSalesMartV2.py
    ``fix: refund condition outlet & fix bug``
    - ProductMart.py
    ``fixing get data conditioning for non-refund transaction``
    - BusiestTimeMart.py
    ``fix: refund condition product & busiest selling time``
    - ProductV2
        - Query.py
        ``fix: refund condition product & busiest selling time``

## [1.24.1] - 2023-07-18
- fix/outlet-hpp-addon [wikan.kuncara@majoo.id]
### Changed
- Models
    - OutletSalesMartV2.py
    ``fix: outlet hpp addon``

## [1.24.0] - 2023-07-07
- enhancement/health-check [muhammad.muzakki@majoo.id]
- feature/etl-compliment-prod [abdur.rasyid@majoo.id]
- coldfix/subextreas-int-expected [akbar.noto@majoo.id]
### Changed
- Controller
    - Heatlh.py `added spark health/condition checking`
- Models
    - Extras/Raw.py `fix month & yearly float data type, cast to int`
### Fixed
- Model
    - ComplimentMart.py
        - processComplimentParamBegEndTime()
        ``add group by transaction_id in dml child``
        - processComplimentParam()
        ``add group by transaction_id in dml child``

## [1.23.0] - 2023-07-05
- feature/harian [akbar.noto@majoo.id]
- coldfix/add-delay-subextras [akbar.noto@majoo.id]
- coldfix/add-delay-on-cashier [akbar.noto@majoo.id]
- coldfix/add-delay-on-promo [akbar.noto@majoo.id]
### Added
- Controller
    - DailyReport.py
- Models
    - Daily/Spark/*.py `ETL via spark`
    - Daily/Pandas/*.py `ETL via Pandas`
### Changed
- Controller
    - ExtraSales.py `add delay parameter, to get older data when realtime, default 600s`
    - SalesEachCashier.py `Add delay 600s default, configureable`
    - SalesPromoCouponPoin.py `add default delay 600, configurable via api`
- Models
    - EachCashierMart.py  `Add delay 600s default, configureable`
    - ExtraMart.py `Add if realtime use delay (default 600s) else 5 seconds & minor bugfix`
    - Extras.Raw,py `remove print`
    - PromoCouponPointMart.py `add default delay 600, configurable via api`
- main.py `Add endpoints for daily report`

## [1.22.0] - 2023-07-04
- feature/etl-outlet-sales-v2 [wikan.kuncara@majoo.id]
### Added
- Controller
    - OutletSalesV2.py
- Models
    - OutletSalesMartV2.py
### Changed
- main.py
``feature: outlet sales v2``

## [1.21.0] - 2023-06-27
- feature/utilisasi [abdur.rasyid@majoo.id]
### Added
- Controller
    - Utilization.py
- Models
    - UtilizationMart.py
### Changed
- main.py
`` added endpoint for utilization``

## [1.20.1] - 2023-06-26
- hotfix/adjustment-spark-specs [muhammad.muzakki@majoo.id]
- Ahmad-Irsyadur-Roziqin/jenkinsfile-edited-online-with-bitbucket-1676469329085 [ahmad.roziqin@majoo.id]
### Changed
- helper.py
`` upgrade spark memory``
- Jenkinsfile
`` remove unnecessary processes``

## [1.20.0] - 2023-06-23
- feature/etl-snbn [muhammad.muzakki@majoo.id]
### Added
- Controller
    - BatchSerialNumber.py
- Models
    - BatchSerialNumber
        - BatchSerialNumberMart.py
### Changed
- main.py
`` added endpoint for snbn``
- helper.py
`` added function to get inventory database credential``

## [1.19.4] - 2023-06-22
- coldfix/add-cabang-on-join-akunting [akbar.noto@majoo.id]
- fix/etl-product-v2-monthly-where-date [wikan.kuncara@majoo.id]
### Changed
- Models
    - Akunting
        - Pandas
            - RawService.py
            `` Add cabang in Join Query to Optimize Extract from MySQL Akunting``
    - AkuntingMart.py
    `` Add cabang in Join Query to Optimize Extract from MySQL Akunting``
    - ProductMartV2.py
    ``fix: etl product v2 monthly where date query``

## [1.19.3] - 2023-06-21
- feature/etl-order-type [muhammad.muzakki@majoo.id]
### Changed
- Models
    - OrderType
        - OrderTypeFunction.py
            - getDatamartParameters()
            ``remove raw_mart_order_type_multi_price_type column from recalculation filter``

## [1.19.2] - 2023-06-20
- adjustment vm for etl akunting [ahmad.roziqin@majoo.id]
- coldfix/subextra-uncleaned-deletion [akbar.noto@majoo.id]
### Changed
- Models
    - Extras
        - Raw.py
        ``add param tobedeletedSubextraMonthly & tobedeletedSubextraYearly for delete``
- Jenkinsfile
``adjustment ip & port ssh ke etl akunting prod``

## [1.19.1] - 2023-06-17
- adjustment port [ahmad.roziqin@majoo.id]
### Changed
- Jenkinsfile
``adjustment port ssh ke etl akunting prod``

## [1.19.0] - 2023-06-15
- coldfix/subextras [akbar.noto@majoo.id]
- feature/etl-order-type [muhammad.muzakki@majoo.id]
- fix/etl-product-v2-df-name [wikan.kuncara@majoo.id]
### Added
- Controller
    - OrderType.py
- Models
    - OrderType
        - OrderTypeMart.py
        - OrderTypeMartFunctions.py
### Changed
- Models
    - Extras
        - PandasService.py
        ``fix query delete``
        - Raw.py
        ``fix query delete``
    - ProductMartV2.py
    ``fix: etl product df name``
- main.py
``add route order type, remove route migration``
### Removed
- Controller
    - Migration.py
- Models
    - Migration.py

## [1.18.0] - 2023-06-13
- feature/apm [akbar.noto@majoo.id]
- feature/extra-subextra [akbar.noto@majoo.id]
- fix/etl-waktu-teramai-penjualan-duplicate [wikan.kuncara@majoo.id]
### Added
- Middleware
    - ElasticAPMMiddleware.py
- Utility
    - apm.py
- helper.py
    - getElasticAPMConfig()
- requirements.txt
    - elastic-apm==6.15.1
### Changed
- Models
    - BusiestTimeMart.py
        - processBusiestTimeParamBegEndTime()
        ``add treshold from 1s to 10s, move handle param t_endupdatedate``
    - ExtraMart.py
        - processParam()
        - processParamWithPandas()
        ``add parameter id_cabang``
    - Extras
        - PandasService.py
            - getRecoveringCabang()
            ``change force param recoveredCabang to string``
            - deleteTargeted()
            ``add param input recoveryAt``
        - Raw.py
            - getRecoveringCabang()
            ``change force param recoveredCabang to string``
        - SubExtra.py
            - fetchTransformDaily()
            - fetchTransformHourly()
            - fetchTransformMonthly()
            - fetchTransformYearly()
            - deleteTargetedMonthly()
            - deleteTargetedYearly()
            - fetchTransformTargetedMonthly()
            - fetchTransformTargetedYearly()
            ``add param idUser & idCabang``
- main.py
``add config for use APM middlewate``

## [1.17.0] - 2023-06-09
- migrasi GCP [ahmad.roziqin@majoo.id]
### Changed
- Jenkinsfile
``change deployment to gcp``

## [1.16.0] - 2023-06-08
- feature/extra-subextra [akbar.noto@majoo.id]
- feature/akunting-in-pandas [akbar.noto@majoo.id]
- hotfix/recovery-addon-detail [akbar.noto@majoo.id]
- hotfix/forceindex-name-diff-akunting [akbar.noto@majoo.id]
### Added
- Controller
    - Akunting.py
        - sequencePerMinWithPandas()
        - recoveryWithPandas()
    - ExtraSales.py
- Models
    - Akunting
        - Pandas
            - Cashflow.py
            - RawService.py
            - Saldo.py
    - Extras
        - PandasService.py
        - Raw.py
        - SubExtra.py
    - AkuntingMart.py
        - processAkuntingBegEndTimeWithPandas()
        - recoveryWithPandas()
    - ExtraMart.py
- Utility
    - Pandas
        - PandasService.py
### Changed
- Controller
    - Akunting.py
        - sequencePerMinWithPandas()
        - recoveryWithPandas()
- Models
    - AddonDetailMart.py
        - processSalesProductParam()
        ``fix query delete raw mart``
    - AkuntingMart.py
        - processAkuntingParamBegEndTime()
        ``remove force index``ß
        - processAkuntingSequencePerMin()
        ``add param input 'via'``
- main.py
``add route for etl extra sub extra revamp``
``add route for etl akunting with pandas``
- requirment.py
``add connector, pyarrow, SQLAlchemy``
- utility
    - sql.py
        - createETLLog()
        ``add param input moreColumns``

## [1.15.2] - 2023-05-25
- feature/etl-compliment-prod [abdur.rasyid@majoo.id]
- feature/adaptive-spec-delay-on-akunting [akbar.noto@majoo.id]
### Changed
- Controller
    - Akunting.py
    ``add param delay``
- Models
    - ComplimentMart.py
        - processComplimentParamBegEndTime()
        - processComplimentParam()
        ``fix handle timezone +7 on source``
    - AkuntingMart.py
    ``add param delay, and change driver memory usage``

## [1.15.1] - 2023-05-09
- fix/speed-up-etl-product [muhammad.muzakki@majoo.id]
- feature/etl-compliment-prod [abdur.rasyid@majoo.id]
### Changed
- Models
    - ProductMart.py
        - processTransactionProductParamBegEndTime()
        ``change treshold delay from 5s to 60s``
    - ComplimentMart.py
        - processComplimentParamBegEndTime()
        - processComplimentParam()
        ``fix handle timezone +7``

## [1.15.0] - 2023-05-09
- feature/etl-compliment-prod [abdur.rasyid@majoo.id]
- feature/etl-busiest-item-time-postgres [muhammad.muzakki@majoo.id]
- feature/etl-reservation-prod [abdur.rasyid@majoo.id]
### Added
- Controller
    - Compliment.py
- Models
    - ComplimentMart.py
### Changed
- Controller
    - BusiestItemTime.py
    ``penambahan param dinamis delay in sec``
- Models
    - BusiestItemTime
        - BusiestItemTimeMart.py
        ``penambahan param dinamis delay in sec``
    - ReservationMart.py
        - processSalesReservationParamBegEndTime()
        - processSalesReservationParam()
        ``change status only proces status = 700 (complete)``
- main.py
``add route for etl komplimen``

## [1.14.1] - 2023-05-05
- coldfix/akunting-status-14 [akbar.noto@majoo.id]
- feature/etl-busiest-item-time-postgres [muhammad.muzakki@majoo.id] [revert]
- feature/nrow-update [wikan.kuncara@majoo.id]
- feature/paralelisasi-kasir-promo [akbar.noto@majoo.id]
- feature/etl-reservation-prod [abdur.rasyid@majoo.id]
### Added
- Models
    - BusiestItemTime
        - _BusiestItemTimeMart v1.1.py
        - _BusiestItemTimeMart_v1.py
        - BusiestItemTimeMart.py
### Changed
- Controller
    - BusiestItemTime.py
    ``change name of model``
    - BusiestTime.py
    ``add param input n_size``
    - OutletSales.py
    ``add param input n_size``
- Models
    - AkuntingMart.py
    - JurnalAkuntingMart.py
    ``add filter status 14``
    - BusiestTimeMart.py
    ``add param input n_size``
    - OutletSalesMart.py
    ``add param input n_size``
    - EachCashierMart.py
    ``delete at once & insert each table``
    - CouponMart.py
    - PromoCouponPoinMart.py
    - PromoMart.py
    ``use paralelisasi``
- requirements.txt
``add SQLAlchemy==2.0.10``
- Utility
    - sql.py
    ``memecah fungsi create connection``
- helper.py
``add config for db config GCP reservasi``

## [1.14.0] - 2023-04-28
- feature/etl-reservation-prod [abdur.rasyid@majoo.id]
### Added
- Controller
    - Reservation.py
- Models
    - ReservationMart.py
### Changed
- main.py
``add route for etl reservasi``

## [1.13.0] - 2023-04-26
- feature/etl-product-col-refund-postgres [wikan.kuncara@majoo.id]
- feature/outlet-sales-chunk [wikan.kuncara@majoo.id]
### Added
- Controller
    - ProductV2.py
- Models
    - ProductMartV2.py
    - ProductV2
        -Query.py
### Changed
- Model
    - OutletSalesMart.py
    ``feature: add chunk to etl outlet sales``
- main.py
``add route for etl produk v2``

## [1.12.6] - 2023-04-20
- coldfix/data-delay-on-jurnal [akbar.noto@majoo.id]
### Changed
- Controller
    - JurnalAkunting.py
    ``penambahan param dinamis delay in sec``
- Model
    - JurnalAkunting.py
    ``penambahan param dinamis delay in sec``

## [1.12.5] - 2023-04-19
- improvement/akunting-query-join [akbar.noto@majoo.id]
### Changed
- Model
    - AkuntingMart.py
        - processAkuntingParam()
        ``rollback recovery``

## [1.12.4] - 2023-04-18
- feature/hotfix-dftemp-akunting [akbar.noto@majoo.id]
### Fixed
- Model
    - AkuntingMart.py
        - processAkuntingParam()
        ``fix df_temp recovery``

## [1.12.3] - 2023-04-12
- fix/etl-product-duplicate-error [wikan.kuncara@majoo.id]
### Fixed
- Model
    - ProductMart.py
    ``fix error duplicate karena undefined timezone``

## [1.12.2] - 2023-04-11
- feature/mart-journal-daily [akbar.noto@majoo.id]
- bug/sales-product [m.miftakhul@majoo.id]
### Changed
- Model
    - JurnalAkuntingMart.py
    ``change query for get field id``
    - ProductMart.py
    ``reposition spark variable``

## [1.12.1] - 2023-04-06
- feature/mart-journal-daily [akbar.noto@majoo.id]
- fenhancement/datamart-insert-method [muhammad.muzakki@majoo.id]
### Changed
- Model
    - JurnalAkuntingMart.py
    ``cast as string to fix jsonstring format``
    - BusiestItemTimeMart.py
    - BusiestTimeMart.py
    - CouponMart.py
    - EachCashierMart.py
    - ProductMart.py
    - PromoMart.py
    ``improve insert method``

## [1.12.0] - 2023-04-04
- feature/etl-outlet-sales-postgres [wikan.kuncara@majoo.id]
### Added
- Controller
    - OutletSales.py
- Model
    - OutletSalesMart.py
- main.py
``add route for etl laporan outlet postgres``

## [1.11.0] - 2023-03-31
- feature/laporan-promo-kupon-poin-postgres [akbar.noto@majoo.id]
- improvement/akunting-query-join [akbar.noto@majoo.id]
### Added
- Controller
    - SalesPromoCouponPoin.py
- Model
    - CouponMart.py
    - PromoCouponPoinMart.py
    - PromoMart.py
    - Akunting
        - Cahsflow.py
        - Saldo.py
        ``improvement partial query join``
    - AkuntingMart.py
    ``improvement partial query join``
### Changed
- main.py
``add route for laporan promo kupon poin postgres``

## [1.10.1] - 2023-03-30
- improvement/akunting-postgres-filtered-batch [akbar.noto@majoo.id]
- fix/speed-up-etl-product [muhammad.muzakki@majoo.id]
### Changed
- Controller
    - Product.py
        - withParameter()
        ``add parameter for set spark config manual``
- Model
    - Akunting
        - RDD.py
        ``fixing code``
    - AkuntingMart.py
    ``change confiq default spark``
    - ProductMart.py
        - processTransactionProductParam()
        ``add proces paralelism for recovery``

## [1.10.0] - 2023-03-29
- improvement/akunting-postgres-filtered-batch [akbar.noto@majoo.id]
- feature/google-space-alert [akbar.noto@majoo.id]
### Added
- notification.py
- helper.py
    - getSpaceWebhook()
    - getENV()
### Changed
- Controller
    - Akunting.py
    ``delete method withParamSaldo()``
- Model
    - Akunting
        - Cashflow.py
        - Saldo.py
        ``fetch data paralelizations``
    - AkuntingMart.py
    ``fix job placements, add compute monthly yearly on recovery``
- main.py
``remove route akunting-with-param-saldo n akunting-mart-user-daily, add web hook gspace``

## [1.9.0] - 2023-03-25
- feature/mart-journal-daily [akbar.noto@majoo.id]
### Added
- Controller
    - JurnalAkunting.py
- Model
    - JurnalAkuntingMart.py
### Changed
- main.py
``add router for API ETL journal``

## [1.8.4] - 2023-03-17
- feature/migrasi-iap-cogs [muhammad.muzakki@majoo.id]
- improvement/akunting-postgres-filtered-batch [akbar.noto@majoo.id]
### Changed
- Controller
    - Migration.py
    ``added spark spec configuration for akunting VM``
- Model
    - Migration.py
    ``added spark spec configuration for akunting VM``
    - AkuntingMart.py
        - processAkuntingParamBegEndTime()
        ``add logic for filtered batch montly and yearly``
    - Akunting
        - Cashflow.py
        - RDD.py
        - Saldo.py
        ``add logic for filtered batch montly and yearly``
- Utility
    - sql.py
    ``add parameter fetchone``

## [1.8.3] - 2023-03-09
- fix/speed-up-etl-product [muhammad.muzakki@majoo.id]
### Changed
- Model
    - ProductMart.py
        - processTransactionProductSequencePerMin()
        - processTransactionProductParamBegEndTime()
        ``update insert method + group by order``

## [1.8.2] - 2023-03-08
- fix/speed-up-etl-product [muhammad.muzakki@majoo.id]
- feature/migrasi-iap-cogs [muhammad.muzakki@majoo.id]
### Changed
- Controller
    - Product.py
        - sequencePerMin()
        ``added dynamic spec settings``
- Model
    - ProductMart.py
        - processTransactionProductSequencePerMin()
        - processTransactionProductParamBegEndTime()
        ``added dynamic spec settings``
    - Migration.py
        - iapCogs()
        ``change insert method on iap migration table``

## [1.8.1] - 2023-03-07
- feature/etl-busiest-item-time-postgres [muhammad.muzakki@majoo.id]
- fix/speed-up-etl-product [muhammad.muzakki@majoo.id]
### Added
- Utility
    - sql.py
        - insertUsingFile()
### Changed
- Model
    - BusiestItemTimeMart.py
        - processBusiestItemTimeParamBegEndTime()
        - generateDatamart()
        ``optimized parallel process``
    - ProductMart.py
        - processTransactionProductParamBegEndTime()
        ``added distinct at generate hourly and daily process``

## [1.8.0] - 2023-03-06
- feature/migrasi-iap-cogs [muhammad.muzakki@majoo.id]
- fix/speed-up-etl-product [muhammad.muzakki@majoo.id]
- feature/turn-off-journal-mart-spark [akbar.noto@majoo.id]
### Added
- Controller
    - Migration.py
- Model
    - Migration.py
        - iapCogs()
- helper.py
``add config db COGS``
- main.py
``add route for api migration``
### Changed
- Model
    - ProductMart.py
    ``add modul concurent``
        - processTransactionProductParamEndTimePeriod()
        - processTransactionProductParamBegEndTime()
        ``add logic untuk runing flow concurent``
    - AkuntingMart.py
        - processAkuntingParamBegEndTime()
        - processAkuntingParam()
        ``remove proses marting Journal daily``

## [1.7.0] - 2023-02-24
- improvement/akunting-postgres [akbar.noto@majoo.id]
- feature/specification-param-cashier [akbar.noto@majoo.id]
### Changed
- Controller
    - SalesEachCashier.py
    ``add param input for spec spark``
- Model
    - AkuntingMart.py
        - processAkuntingParamEndTimePeriod()
        - processAkuntingParam()
        - processAkuntingSaldoParam()
        ``spec adjustment spark for prod akunting``
    - SalesEachCashier.py
        - processSalesProductSequencePerMin()
        - processSalesProductParamBegEndTime()
        ``add param input for spec spark``

## [1.6.8] - 2023-02-21
- Ahmad-Irsyadur-Roziqin/jenkinsfile-edited-online-with-bitbucket-1676534063517 [ahmad.roziqin@majoo.id]
### Changed
- Jenkinsfile
``add port for new image for etl akunting new vm``

## [1.6.7] - 2023-02-16
- Ahmad-Irsyadur-Roziqin/jenkinsfile-edited-online-with-bitbucket-1676469329085 [ahmad.roziqin@majoo.id]
### Changed
- Jenkinsfile
``add new image for etl akunting new vm``

## [1.6.6] - 2023-02-14
- coldfix/fix-mart-cashier [akbar.noto@majoo.id]
### Changed
- Model
    - EachCashierMart.py
        - processSalesProductParamBegEndTime()
        - processSalesProductParam()
        ``fix cashier flow + collect rdd first then delete``
- helper.py
``helper for productions``

## [1.6.5] - 2023-02-06
- hotfix [grandis@majoo.id]
### Changed
- Model
    - ProductMart.py
        - processTransactionProductSequencePerMin()
        - processTransactionProductParamBegEndTime()
        ``add param n_size for insert and delete default 500, and param idStore``
- Controller
    - Product.py
    ``add param size for insert n delete``

## [1.6.4] - 2023-02-01
- feature/coldfix-undeleted-deleted-transactions [akbar.noto@majoo.id]
- fix/spark-spesification-prod-4 [muhammad.muzakki@majoo.id]
- improvement/akunting-postgres [akbar.noto@majoo.id]
### Added
- Model
    - Cashflow.py
    - Jurnal.py
    - RDD.py
    - Saldo.py
### Changed
- Model
    - EachCashierMart.py
        - processSalesProductParamBegEndTime()
        - recoveryMartFromRaw()
        ``fix to delete 'deleted transactions'``
    - AkuntingMart.py
    ``improve flow and query etl akunting``
- Controller
    - Akunting.py
    ``add param size for insert n delete``
### Fixed
- helper.py
``update CPU configuration from 4 core to 7 core``

## [1.6.3] - 2023-01-30
- hotfix [grandis@majoo.id]
### Changed
- Model
    - ProductMart.py
        - processTransactionProductParamBegEndTime()
        - processTransactionProductParam()
        ``implement cache``

## [1.6.2] - 2023-01-25
- feature/etl-busiest-item-time-postgres [muhammad.muzakki@majoo.id]
### Changed
- Model
    - BusiestItemTimeMart.py
        - processBusiestItemTimeParamBegEndTime()
        - processBusiestItemTimeParam()
        ``removed datamart daily and added chunk process to get transaction detail data``

## [1.6.1] - 2023-01-24
- feature/busiest-selling-time-parallel [wikan.kuncara@majoo.id]
### Changed
- Model
    - BusiestTimeMart.py
        - processBusiestTimeParamBegEndTime()
        - processBusiestTimeParam()
        ``add parallel process in busiest selling time``

## [1.6.0] - 2023-01-23
- enhancement [grandis@majoo.id]
### Changed
- Model
    - ProductMart.py
        - processTransactionProductParamBegEndTime()
        - processTransactionProductParam()
        ``added a parallel process feature to generate datamart``

## [1.5.12] - 2023-01-20
- feature/etl-busiest-item-time-postgres [muhammad.muzakki@majoo.id]
### Fixed
- Model
    - BusiestItemTimeMart.py
    ``added a parallel process feature to generate datamart``
- requirment.txt
``added module futures3``

## [1.5.11] - 2023-01-18
- feature/etl-busiest-item-time-postgres [muhammad.muzakki@majoo.id]
### Fixed
- Model
    - BusiestItemTimeMart.py
    ``add cache to improve the ETL performance``

## [1.5.10] - 2023-01-16
- hotfix [grandis@majoo.id]
### Fixed
- Model
    - ProductMart.py
        - processTransactionProductParamBegEndTime()
        - processTransactionProductParam()
        ``fix query for field raw_mart_sales_product_date, add n_size_delete = 50, unpersist dataframe``

## [1.5.9] - 2023-01-13
- hotfix [grandis@majoo.id]
### Fixed
- Model
    - AddonDetailMart.py
        - processSalesProductParam()
        ``remove quote in param M_User_id_user``

## [1.5.8] - 2023-01-12
- coldfix/scalling-down-cashier-chunk-size [akbar.noto@majoo.id]
### Fixed
- Model
    - EachCashierMart.py
    ``scalling down chunk from 1000 to 300``

## [1.5.7] - 2023-01-11
- hotfix/etl-busiest-selling-time-chunk-select [wikan.kuncara@majoo.id]
### Fixed
- Model
    - BusiestTimeMart.py
    ``fix unpersist dataframe, chunk per 100 transaction on select from transaction detail``

## [1.5.6] - 2023-01-09
- hotfix/etl-busiest-selling-time-chunk-select [wikan.kuncara@majoo.id]
- feature/etl-busiest-item-time-postgres [muhammad.muzakki@majoo.id]
### Fixed
- Model
    - BusiestTimeMart.py
    ``fix chunk select on etl busiest selling time``
    - BusiestItemTimeMart.py
    ``increase batch size``

## [1.5.5] - 2023-01-05
- feature/etl-trx-payment-type-postgres [m.miftakhul@majoo.id]
### Fixed
- Model
    - TrxPaymentMart.py
    ``fix get date interval``
    - TrxTypeMart.py
    ``fix get date interval``

## [1.5.4] - 2023-01-03
- feature/etl-trx-payment-type-postgres [m.miftakhul@majoo.id]
- coldfix/cashier-fix-null-in-string [akbar.noto@majoo.id]
### Fixed
- Model
    - TrxPaymentMart.py
    - TrxTypeMart.py
    ``Adjust time when get start & end date, Add url spark for insert``
    - EachCashierMart.py
    ``fix null in string fix on no payment,fix uncleared cache``

## [1.5.3] - 2023-01-02
- feature/etl-payment-postgres [m.miftakhul@majoo.id]
### Fixed
- Model
    - TrxPaymentMart.py
    ``fix duplicate unique key``

## [1.5.2] - 2022-12-30
- feature/etl-type-postgres [m.miftakhul@majoo.id]
- fix/spark-specification-prod-2 [muhammad.muzakki@majoo.id]
- feature/etl-payment-postgres [m.miftakhul@majoo.id]
### Fixed
- Model
    - TrxTypeMart.py
    ``optimize loop & fix usage df_updatedate.first()``
    - TrxPaymentMart.py
    ``fix duplicate unique key``
- helper.py
``adjsutment spark specs (VM specs : 4C 32GB)``

## [1.5.1] - 2022-12-28
- coldifx/cache-on-akunting-postgres [akbar.noto@majoo.id]
- feature/etl-type-postgres [m.miftakhul@majoo.id]
- fix/close-connection-db [muhammad.muzakki@majoo.id]
### Fixed
- Model
    - TrxTypeMart.py
    ``Add cast to query for change status from varchar to int``
    - AkuntingMart.py
    ``add cache on akunting``
- Utility
    - sql.py
    ``added close connection process for postgres database call function``

## [1.5.0] - 2022-12-27
- feature/etl-sales-product-postgres [m.miftakhul@majoo.id]
### Added
- Controller
    - Product.py
- Model
    - ProductMart.py
### Changed
- main.py
``add route for etl laporan product new metrix postgres``

## [1.4.1] - 2022-12-26
- hotfix [grandis@majoo.id]
- fix/etl-busiest-selling-time-postgres [wikan.kuncara@majoo.id]
- feature/etl-type-postgres [m.miftakhul@majoo.id]
### Fixed
- Model
    - AddonDetailMart.py
    - AddonMart.py
    - AkuntingMart.py
    - BusiestItemTimeMart.py
    - SupportTrxMart.py
    - TransactionMart.py
    ``fix usage df_updatedate.first()``
    - BusiestTimeMart.py
    ``fix usage df_updatedate.first()``
    ``rename dataframe api recovery, add table schema``
    - TrxTypeMart.py
    ``Add method to recalculate, Convert data type to join``

## [1.4.0] - 2022-12-20
- feature/health-check [muhammad.muzakki@majoo.id]
- feature/etl-busiest-item-time-postgres [muhammad.muzakki@majoo.id]
- hotfix/datetime-to-date-postgres-recovery [akbar.noto@majoo.id]
### Added
- Controller
    - Health.py
### Changed
- main.py
``add route for api health check``
- helper.py
``add config for nifi env``
- requirment.txt
``add requests==2.28.1``
### Fixed
- Model
    - AddonDetailMart.py
    - AddonMart.py
    - EachCashierMart.py
    - TransactionMart.py
        - processSalesProductParam()
        ``fix query from filter datetime to date``
    - BusiestItemTimeMart.py
    ``optime query and fix recovery``

## [1.3.0] - 2022-12-14
- feature/laporan-per-kasir-postgres [akbar.noto@majoo.id]
### Added
- Controller
    - SalesEachCashier.py
- Model
    - EachCashierMart.py
### Changed
- helper.py
``import pytz & datetime``
- main.py
``add route for etl laporan per kasir postgres``
- sql.py
``fix timestampz + convert postgres``
### Fixed
- Model
    - AddonDetailMart.py
    ``fix recovery from transaction_tgl``

## [1.2.2] - 2022-12-09
- fix/spark-specification-prod [muhammad.muzakki@majoo.id]
### Changed
- helper.py
``fix spark specification in vm``

## [1.2.1] - 2022-12-02
- fix/spark_master_configuration [muhammad.muzakki@majoo.id]
### Changed
- common.sh
- spark-defaults.conf
- spark-master.sh
- spark-worker.sh
``fix spark configuration in k8s``

## [1.2.0] - 2022-11-21
- feature/etl-busiest-item-time-postgres [muhammad.muzakki@majoo.id]
### Added
- Controller
    - BusiestItemTime.py
- Model
    - BusiestItemTimeMart.py
### Changed
- main.py
``add route for etl busiest item time postgree``

## [1.1.1 ] - 2022-11-16
- fix/timestampz-etl-postgres [muhammad.muzakki@majoo.id]
### Fixed
- Model
    - ActivitySalesLeadMart.py
    - AddonDetailMart.py
    - AddonMart.py
    - AkuntingMart.py
    - BusiestTimeMart.py
    - MerchantOutletMart.py
    - SubmissionEcommerceGrabfoodMart.py
    - SubmissionWalletMart.py
    - SupportTrxMart.py
    - TransactionMart.py
    - TransactionWalletDynamicMart.py
    - TrxPaymentMart.py
    - TrxTypeMart.py
    ``eror timestamp when iteration``
    ``key duplication``

## [1.1.0] - 2022-11-10
- fix/etl-postgres-timezone [muhammad.muzakki@majoo.id]
- feature/pg-etl-submission-ecommerce-grabfood [vera@majoo.id]
### Changed
- Model
    - SubmissionEcommerceGrabfoodMart.py
    ``change structure``
    ``split transaction and submission``
    ``adjust query to fit in postgres``
### Fixed
- Model
    - ActivitySalesLeadMart.py
    - AddonDetailMart.py
    - AddonMart.py
    - AkuntingMart.py
    - BusiestTimeMart.py
    - MerchantOutletMart.py
    - SubmissionEcommerceGrabfoodMart.py
    - SubmissionWalletMart.py
    - SupportTrxMart.py
    - TransactionMart.py
    - TrxPaymentMart.py
    - TrxTypeMart.py
    ``iterations are always in the same time range``
    ``key duplication``

## [1.0.0] - 2022-10-19
### Added
- Release migrasi etl from mysql to postgree
