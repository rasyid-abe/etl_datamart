
---

# Installation

## Tutorial install spark Windows

https://docs.google.com/document/d/1c9I13g8brYcFKzzsmbFJS0LsDjz66VyvISvX3QiyJr4/edit
https://phoenixnap.com/kb/install-spark-on-windows-10

Download This File 
https://drive.google.com/drive/folders/1C3GX4U6UXhtvb4Gj8UR8j4U_qZUYPh_v?usp=sharing

Paste in folder jars (Spark)

## Tutorial install spark MacOs

1. running “xcode-select --install “ in terminal
2. running “/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)”” in terminal
3. running “brew install python” in terminal
4. restart terminal and check "python —version” and show python with version 3.*.*
5. install java8 with “brew install —cask adoptopenjdk8” in your terminal
6. download apache spark 3.0.3 in https://spark.apache.org/downloads.html and choose package type “pre built for apache hadoop 2.7”
7. extract file in your folder ../Documents/Spark
8. run pip install pyspark==3.0.3
9. run in ur terminal “nano .zshrc”
10. copy this text, and paste
    export SPARK_HOME=/Users/macbookpro/Documents/Spark/spark-3.0.3-bin-hadoop2.7
    export PATH=$SPARK_HOME/bin:$PATH
    export JAVA_HOME=/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home
    #For python 3, You have to add the line below or you will get an error
    export PYSPARK_PYTHON=python3
    export PATH=$JAVA_HOME/bin:$PATH
    export PATH 
11. save, and run “source .zshrc”. Restart terminal


# After installation spark & python success

1. Install pip falcon.
2. Install pip falcon middleware.
3. Install pip pyJWT.
4. Install pip falcon-auth.
5. Install pip falcon-cors.
6. Install pip falcon-multipart.
7. pip install pyspark

---

## [1.0.0] - 2021-09-29

- release project etl for report product

## [1.1.0] - 2022-02-10

- feature/etl-varian
``add feature etl for varian``
``setting env in file .env``

## [1.1.1] - 2022-04-12

``rollback use plan env coz CICD not ready``
``fix range filter updatedate``
``rename df for method recovery``

## [1.1.2] - 2022-04-28

``enhance tools recovery for sales product & addon``

## [1.2.0] - 2022-05-19

- feature/log-etl
``add feature logging``
- remove plan env and use dotenv

## [1.3.0] - 2022-05-26

- feature/etl-akunting
``add feature etl akunting``

## [1.3.1] - 2022-06-09

- hotfix/etl-akunting-input-param-manual-ambigous-id-user
``fix ambigous query for idStore``

## [1.4.0] - 2022-06-10

- feature/addons-detail-optimization
``add feature etl sub varian``

## [1.4.1] - 2022-06-20

- hotfix
``change filter j.jur_tgl and add config in connection jdbc``

## [1.4.2] - 2022-06-27

- bugfix-data-overlay
    - Model
        - AddonDetailMart.py
        - AddonlMart.py
        - AkuntingMart.py
        - TransactionMart.py
        ``param sCurent minus 1s``
- coldfix-logging
``fix loging warning, set all config spark to helper``

## [1.4.3] - 2022-07-15

- coldfix-addon-detail-history
    - Model
        - AddonDetailMart.py
            - processSalesProductParamEndTimePeriod()
            ``change name table from raw_datamart_addon_history to raw_datamart_addon_detail_history``
- coldfix-modal-addon
    - Model
        - AddonDetailMart.py
            - processSalesProductParamBegEndTime()
            ``change logic for raw_mart_sales_addon_detail_price_modal from (transaction_addon_detail_quantity * transaction_addon_detail_harga_modal) to (transaction_addon_detail_quantity * transaction_addon_detail_quantity_bahan * transaction_addon_detail_harga_modal)``

## [1.4.4] - 2022-07-28

- coldfix-turnoff-traceback
    - logger.py
    ``comment for param traceback``

## [1.4.5] - 2022-07-31

- bugfix/add-status-cancel-n-minus-2-second
``change minus time from 1 second to 2 second``
``change filter status from ('1','9') to ('1','4','9')``

## [1.4.6] - 2022-08-01

``change minus time from 2 second to 5 second`
- bugfix/change-recovery-can-only-date
``can handle if parameter input email null will be recovery all user``

## [1.4.7] - 2022-08-03

- feature/sparklog-dockerfile
``change log spark to log4j2, change spark log to json``
- feature/addon-detail-runidstore
``handle iterasi for run few user``

## [1.5.0] - 2022-08-04

``fix etl stagnant in etl produk & varian``
- feature/etl-merchant-outlet
``add feature etl merchant outlet``

## [1.6.0] - 2022-08-08

- feature/etl-support-trx
``add feature etl support transaction``
- feature/etl-trx-type
``add feature etl transaction type``

## [1.6.1] - 2022-08-11

- hotfix
``fix add query filter muhsa.is_active = 1 in feature etl-support-trx``
``change conf spark, use from helper.py``

## [1.6.2] - 2022-08-15

- hotfix
``fix add query filter Support_transactions_exp_date > '1970-01-01 00:00:01' in feature etl-support-trx for prevent '0000-00-00 00:00:00'``
``fix etl recovery akunting, when delete data use filter range date``


## [1.7.0] - 2022-08-22

- feature/recovery-saldo-akunting
``add api for recovery saldo akunting``
- hotfix
``add filter date to avoid '0000-00-00'``
``add row_number to prevent duplicate ID``
``add script recovery akunting saldo``

## [1.8.0] - 2022-08-30

- feature/etl-submission-wallet
``add feature etl submission wallet``

## [1.8.1] - 2022-09-01

- bugfix/handle-recovery-if-no-data-in-source
``handle recovery for etl produk, varian, n subvarian if no data in source``

## [1.9.0] - 2022-09-02

- feature/etl-trx-payment
``add feature etl transaction by payment``

## [1.9.1] - 2022-09-08

- bugfix/etl-submission-wallet
``Distinct user_id to avoid redundant, change query summary wallet, list status ordering by updatedate ASC from by updatedate DESC``
- bugfix/etl-trx-payment
``change app name for pyspark coz redundant with existing``

## [1.9.2] - 2022-09-13

- hotfix
``hotfix raise error when error in query, fix query recovery saldo akunting coz wrong field name``

## [1.9.3] - 2022-09-19

- hotfix
``handle recovery saldo akunting if no data``
- bugfix/etl-submission-wallet-fix-latest-status-2
    - Controller
        - SubmissionWallet.py
        ``Add schedule hourly``
    - Model
        - SubmissionWalletMart.py
        ``add processSubmissionWalletHourlyUpdate()``
            - processSubmissionWalletParamBegEndTime()
            ``Change query to get latest status``

## [1.9.4] - 2022-09-21

- hotfix
``etl support trx, change query join table from M_User_has_Support_Active to M_User_has_Support for history record``

## [1.10.0] - 2022-09-22

- feature/etl-submission-ecommerce-grabfood
``add feature etl submission ecommerce grabfood``

## [1.11.0] - 2022-09-23

- feature/etl-trx-wallet-dynamic
``add feature etl trx wallet dynamic``
 
## [1.11.1] - 2022-09-27

- bug/trx-payment-type
``rename spark dataframe etl trx payment type, rename spark dataframe for recovery trx payment type``
- bugfix/etl-submission-ecommerce-grabfood-change-df-name
``rename spark dataframe etl submission ecommerce grabfood``

## [1.11.2] - 2022-10-03
- hotfix
``handle recovery akunting add param sJurnalWhereOutlet``
``Add filter to avoid null on parent id_support_transaction on etl support trx``

## [1.12.0] - 2022-10-07
- bugfix/handle-recovery-if-no-data-in-source
``fixing bug recovery varian & sub varian``
``fixing db connection``
- feature/etl-busiest-selling-time
``add feature etl busiest selling time``

## Unreleased
 - coldfix/add-cabang-on-join-akunting
 `` Add cabang in Join Query to Optimize Extract from MySQL Akunting``