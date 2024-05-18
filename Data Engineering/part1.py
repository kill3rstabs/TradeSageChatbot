import pyodbc
import os

# Connect to MSSQL Server
mssql_conn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-RVRG19P;DATABASE=TradeForesightLLM;')

# Create a cursor object
cursor = mssql_conn.cursor()

# Query to fetch data from DimCountry, FactTrade, DimProduct, FactRankTariff, FactPCI, FactWorldBankData, FactDesta, and FactIMF tables
query = """
    SELECT
        DimCountry.Country,
        DimCountry.Region,
        DimCountry.Subregion,
        DimCountry.CapitalCity,
        DimCountry.Countrycode,
        DimCountry.IsoCode,
        DimCountry.Longitude,
        DimCountry.Latitude,
        DimCountry.Continent,
        DimCountry.StartDate AS CountryStartDate,
        DimCountry.EndDate AS CountryEndDate,
        DimCountry.countryKey,
        DimCountry.IncomeLevelValue,
        DimCountry.LendingTypeValue,
        DimCountry.etldate AS CountryETLDate,
        DimCountry.CountryRegionCode,
        DimCountry.CurrencyArabic,
        DimCountry.Currency,
        DimCountry.IOCCountryCode,
        FactTrade.Legacy_Code,
        FactTrade.ReporterCountryId,
        FactTrade.PartnerCountryId,
        FactTrade.ProductID,
        FactTrade.CreationDateKey,
        FactTrade.Import_Value,
        FactTrade.Export_Value,
        FactTrade.Commodity_ID,
        FactTrade.Classification,
        FactTrade.CREATIONDATE,
        FactTrade.[Trade Flow Code],
        FactTrade.[Trade Flow],
        FactTrade.[Qty Unit Code],
        FactTrade.[Qty Unit],
        FactTrade.Qty,
        FactTrade.[Alt Qty Unit Code],
        FactTrade.[Alt Qty Unit],
        FactTrade.[Alt Qty],
        FactTrade.[Netweight (kg)],
        FactTrade.[Gross weight (kg)],
        FactTrade.[Trade Value (US$)],
        FactTrade.[CIF Trade Value (US$)],
        FactTrade.[FOB Trade Value (US$)],
        FactTrade.Flag,
        FactTrade.Is_Active,
        FactTrade.Snapshot_Date,
        FactTrade.Is_Mirror,
        FactTrade.Landedcost_Export,
        FactTrade.Landedcost_Import,
        FactTrade.Is_MonthAggregate,
        DimProduct.Year,
        DimProduct.ItemId,
        DimProduct.HS02,
        DimProduct.HS02Description,
        DimProduct.HS04,
        DimProduct.HS04Description,
        DimProduct.HS06,
        DimProduct.HS06Description,
        DimProduct.StartDate AS ProductStartDate,
        DimProduct.EndDate AS ProductEndDate,
        DimProduct.HSSource,
        DimProduct.MajorCategory,
        DimProduct.MajorCategoryDescription,
        DimProduct.ISIC_SK,
        DimProduct.Industry,
        DimProduct.[Sub-Industry],
        DimProduct.CreativeGoods,
        DimProduct.HighTechGoods,
        DimProduct.IndustryID_SK,
        DimProduct.ProductDescriptionArabic,
        DimProduct.HS04ProductDescriptionArabic,
        DimProduct.HS02ProductDescriptionArabic,
        FactPCI.FactId,
        FactPCI.Rank,
        FactPCI.ProductCategoryID,
        FactPCI.DateKey,
        FactPCI.[PCI Value],
        FactPCI.Delta,
        FactPCI.ProductName,
        FactPCI.HSVersion,
        FactPCI.HSClassification,
        FactWorldBankData.FactSK,
        FactWorldBankData.DateKey AS WorldBankDateKey,
        FactWorldBankData.IndicatorId,
        FactWorldBankData.IndicatorValue,
        FactWorldBankData.Value,
        FactWorldBankData.Decimal,
        FactWorldBankData.ETLDate AS WorldBankETLDate,
        FactWorldBankData.LegacyID,
        FactWorldBankData.KSACurrencyRate,
        FactDesta.CountryID1_SK,
        FactDesta.CountryID2_SK,
        FactDesta.DateKey AS DestaDateKey,
        FactDesta.Area,
        FactDesta.SubArea,
        FactDesta.Name,
        FactDesta.Variables,
        FactDesta.Variables_Description,
        FactDesta.Values_Description,
        FactIMF.FactID AS IMF_FactID,
        FactIMF.CountryID AS IMF_CountryID,
        FactIMF.IndicatorCode AS IMF_IndicatorCode,
        FactIMF.IndicatorName AS IMF_IndicatorName,
        FactIMF.Unit AS IMF_Unit,
        FactIMF.Value AS IMF_Value,
        FactIMF.Datekey AS IMF_Datekey,
        FactECI.FactId AS ECIFactId,
        FactECI.Rank AS ECIRank,
        FactECI.CountryID AS ECICountryID,
        FactECI.DateKey AS ECIDateKey,
        FactECI.[ECI Value],
        FactECI.Delta AS ECIDelta,
        FactJodi.FactID AS JodiFactID,
        FactJodi.CountryID AS JodiCountryID,
        FactJodi.DateKey AS JodiDateKey,
        FactJodi.Product AS JodiProduct,
        FactJodi.Unit AS JodiUnit,
        FactJodi.Value AS JodiValue,
        FactUNStats.FactID AS UNStatsFactID,
        FactUNStats.CountryID AS UNStatsCountryID,
        FactUNStats.Value AS UNStatsValue,
        FactUNStats.SNA93TableCode,
        FactUNStats.SubGroup,
        FactUNStats.Item AS UNStatsItem,
        FactUNStats.SNA93ItemCode,
        FactUNStats.Series AS UNStatsSeries,
        FactUNStats.Currency AS UNStatsCurrency,
        FactUNStats.SNASystem,
        FactUNStats.FiscalYearType,
        FactUNStats.ValueFootnotes,
        FactUNStats.ETLDate AS UNStatsETLDate,
        FactUNStats.BaseYearDatekey AS UNStatsBaseYearDatekey,
        FactUNStats.CreationDatekey AS UNStatsCreationDatekey,
        FactDTF.FactId,
        FactDTF.DateKey,
        FactDTF.CountryID,
        FactDTF.[Ease of Doing Business Rank],
        FactDTF.[Overall DTF],
        FactDTF.[Starting a Business - Rank],
        FactDTF.[Starting a Business - DTF],
        FactDTF.[Starting a Business - Procedure – Men (number)],
        FactDTF.[Starting a Business - Time – Men (days)],
        FactDTF.[Starting a Business - Cost – Men (% of income per capita)],
        FactDTF.[Starting a Business - Procedure – Women (number)],
        FactDTF.[Starting a Business - Time – Women (days)],
        FactDTF.[Starting a Business - Cost – Women (% of income per capita)],
        FactDTF.[Starting a Business - Paid-in min. capital (% of income per capita)],
        FactDTF.[Dealing with Construction Permits - Rank],
        FactDTF.[Dealing with Construction Permits - DTF],
        FactDTF.[Dealing with Construction Permits - Procedures (number)],
        FactDTF.[Dealing with Construction Permits - Time (days)],
        FactDTF.[Dealing with Construction Permits - Cost (% of warehouse value)],
        FactDTF.[Dealing with Construction Permits - Building quality control index (0-15)],
        FactDTF.[Dealing with Construction Permits - Quality of building regulations index (0-2)],
        FactDTF.[Dealing with Construction Permits - Quality control before construction index (0-1)],
        FactDTF.[Dealing with Construction Permits - Quality control during construction index (0-3)],
        FactDTF.[Dealing with Construction Permits - Quality control after construction index (0-3)],
        FactDTF.[Dealing with Construction Permits - Liability and insurance regimes index (0-2)],
        FactDTF.[Dealing with Construction Permits - Professional certifications index (0-4)],
        FactDTF.[Getting Electricity - Rank],
        FactDTF.[Getting Electricity - DTF],
        FactDTF.[Getting Electricity - Procedures (number)],
        FactDTF.[Getting Electricity - Time (days)],
        FactDTF.[Getting Electricity - Cost (% of income per capita)],
        FactDTF.[Getting Electricity - Reliability of supply and transparency of tariff index (0-8)],
        FactDTF.[Getting Electricity - Total duration and frequency of outages per customer a year (0-3)],
        FactDTF.[Getting Electricity - Mechanisms for monitoring outages (0-1)],
        FactDTF.[Getting Electricity - Mechanisms for restoring service (0-1)],
        FactDTF.[Getting Electricity - Regulatory monitoring (0-1)] AS ElectricityRegulatoryMonitoring,
        FactDTF.[Getting Electricity - Financial deterrents aimed at limiting outages (0-1)] AS ElectricityFinancialDeterrents,
        FactDTF.[Getting Electricity - Communication of tariffs and tariff changes (0-1)] AS ElectricityCommunicationTariffs,
        FactDTF.[Getting Electricity - Price of electricity (US cents per kWh)] AS ElectricityPrice,
        FactDTF.[Registering Property - Rank] AS RegisteringPropertyRank,
        FactDTF.[Registering Property - DTF] AS RegisteringPropertyDTF,
        FactDTF.[Registering Property - Procedures (number)] AS RegisteringPropertyProcedures,
        FactDTF.[Registering Property - Time (days)] AS RegisteringPropertyTime,
        FactDTF.[Registering Property - Cost (% of property value)] AS RegisteringPropertyCost,
        FactDTF.[Registering Property - Quality of the land administration index (0-30)] AS RegisteringPropertyQuality,
        FactDTF.[Registering Property - Reliability of infrastructure index (0-8)] AS RegisteringPropertyReliability,
        FactDTF.[Registering Property - Transparency of information index (0–6)] AS RegisteringPropertyTransparency,
        FactDTF.[Registering Property - Geographic coverage index (0–8)] AS RegisteringPropertyGeographicCoverage,
        FactDTF.[Registering Property - Land dispute resolution index (0–8)] AS RegisteringPropertyLandDisputeResolution,
        FactDTF.[Registering Property - Equal access to property rights index (-2–0)] AS RegisteringPropertyEqualAccess,
        FactDTF.[Getting Credit - Rank] AS GettingCreditRank,
        FactDTF.[Getting Credit - DTF] AS GettingCreditDTF,
        FactDTF.[Getting Credit - Strength of legal rights index (0-12)] AS GettingCreditStrengthLegalRights,
        FactDTF.[Getting Credit - Depth of credit information index (0-8)] AS GettingCreditDepthCreditInformation,
        FactDTF.[Getting Credit - Credit registry coverage (% of adults)] AS GettingCreditCreditRegistryCoverage,
        FactDTF.[Getting Credit - Credit bureau coverage (% of adults)] AS GettingCreditCreditBureauCoverage,
        FactDTF.[Protecting Minority Investors - Rank],
        FactDTF.[Protecting Minority Investors - DTF],
        FactDTF.[Protecting Minority Investors - Extent of conflict of interest regulation index (0-10)],
        FactDTF.[Protecting Minority Investors - Strength of minority investor protection index (0-10)],
        FactDTF.[Protecting Minority Investors - Extent of disclosure index (0-10)],
        FactDTF.[Protecting Minority Investors - Extent of director liability index (0-10)],
        FactDTF.[Protecting Minority Investors - Ease of shareholder suits index (0-10)],
        FactDTF.[Protecting Minority Investors - Ease of shareholder suits index (0-10) old methodology],
        FactDTF.[Protecting Minority Investors - Extent of shareholder governance index (0-10)],
        FactDTF.[Protecting Minority Investors - Extent of shareholder rights index (0-10)],
        FactDTF.[Protecting Minority Investors - Extent of ownership and control index (0-10)],
        FactDTF.[Protecting Minority Investors - Extent of corporate transparency index (0-10)],
        FactDTF.[Paying Taxes - Rank],
        FactDTF.[Paying Taxes - DTF],
        FactDTF.[Paying Taxes - Postfiling index (0-100)],
        FactDTF.[Paying Taxes - Payments (number per year)],
        FactDTF.[Paying Taxes - Time (hours per year)],
        FactDTF.[Paying Taxes - Total tax and contribution rate (% of profit)],
        FactDTF.[Paying Taxes - Profit tax (% of profit)],
        FactDTF.[Paying Taxes - Labor tax and contributions (% of profit)],
        FactDTF.[Paying Taxes - Other taxes (% of profit)],
        FactDTF.[Trading across Borders - Rank],
        FactDTF.[Trading across Borders - DTF],
        FactDTF.[Trading across Borders - Time to export: Border compliance (hours)],
        FactDTF.[Trading across Borders - Cost to export: Border compliance (USD)],
        FactDTF.[Trading across Borders - Time to export: Documentary compliance (hours)],
        FactDTF.[Trading across Borders - Cost to export: Documentary compliance (USD)],
        FactDTF.[Trading across Borders - Time to import: Border compliance (hours)],
        FactDTF.[Trading across Borders - Cost to import: Border compliance (USD)],
        FactDTF.[Trading across Borders - Time to import: Documentary compliance (hours)],
        FactDTF.[Trading across Borders - Cost to import: Documentary compliance (USD)],
        FactDTF.[Enforcing Contracts - Rank],
        FactDTF.[Enforcing Contracts - DTF],
        FactDTF.[Enforcing Contracts - Time (days)],
        FactDTF.[Enforcing Contracts - Cost (% of claim value)],
        FactDTF.[Enforcing Contracts - Quality of judicial processes index (0-18)],
        FactDTF.[Resolving Insolvency - Rank],
        FactDTF.[Resolving Insolvency - DTF],
        FactDTF.[Resolving Insolvency - Recovery rate (cents on the dollar)],
        FactDTF.[Resolving Insolvency - Time (years)],
        FactDTF.[Resolving Insolvency - Cost (% of estate)],
        FactDTF.[Resolving Insolvency - Outcome (0 as piecemeal sale and 1 as going concern)],
        FactDTF.[Resolving Insolvency - Strength of insolvency framework index (0-16)],
        FactDTF.[Resolving Insolvency - Commencement of proceedings index (0-3)],
        FactDTF.[Resolving Insolvency - Management of debtor's assets index (0-6)],
        FactDTF.[Resolving Insolvency - Reorganization proceedings index (0-3)],
        FactDTF.[Resolving Insolvency - Creditor participation index (0-4)],
        FactDTF.Region,
        FactDTF.[Income Group],
        Dim_Date.DateKey,
        Dim_Date.FullDate,
        Dim_Date.FullDate_Ar,
        Dim_Date.DateName,
        Dim_Date.DateNameUS,
        Dim_Date.DateNameEU,
        Dim_Date.DayOfWeek,
        Dim_Date.DayNameOfWeek,
        Dim_Date.DayNameOfWeek_Ar,
        Dim_Date.DayOfMonth,
        Dim_Date.DayOfYear,
        Dim_Date.WeekdayWeekend,
        Dim_Date.WeekOfYear,
        Dim_Date.MonthName,
        Dim_Date.MonthName_Ar,
        Dim_Date.MonthOfYear,
        Dim_Date.IsLastDayOfMonth,
        Dim_Date.CalendarQuarter,
        Dim_Date.CalendarYear,
        Dim_Date.CalendarYearMonth,
        Dim_Date.CalendarYearQtr,
        Dim_Date.FiscalMonthOfYear,
        Dim_Date.FiscalQuarter,
        Dim_Date.FiscalYear,
        Dim_Date.FiscalYearMonth,
        Dim_Date.FiscalYearQtr,
        Dim_Date.AuditKey
    FROM 
        DimCountry
    INNER JOIN 
        FactTrade ON DimCountry.CountryID = FactTrade.ReporterCountryId
    INNER JOIN 
        DimProduct ON FactTrade.ProductID = DimProduct.ProductID_SK
    INNER JOIN
        Dim_Date ON FactTrade.CreationDateKey = Dim_Date.DateKey
    LEFT JOIN
        FactPCI ON DimProduct.ProductID_SK = FactPCI.ProductCategoryID AND FactPCI.DateKey = Dim_Date.DateKey
    LEFT JOIN
        FactWorldBankData ON DimCountry.CountryID = FactWorldBankData.CountryID AND FactWorldBankData.DateKey = Dim_Date.DateKey
    LEFT JOIN
        FactDesta ON (DimCountry.CountryID = FactDesta.CountryID1_SK OR DimCountry.CountryID = FactDesta.CountryID2_SK) AND FactDesta.DateKey = Dim_Date.DateKey
    LEFT JOIN
        FactIMF ON DimCountry.CountryID = FactIMF.CountryID AND FactIMF.DateKey = Dim_Date.DateKey
    LEFT JOIN 
        FactECI ON DimCountry.CountryID = FactECI.CountryID AND FactECI.DateKey = Dim_Date.DateKey
    LEFT JOIN 
        FactJodi ON DimCountry.CountryID = FactJodi.CountryID AND FactJodi.DateKey = Dim_Date.DateKey
    LEFT JOIN
        FactDTF ON DimCountry.CountryID = FactDTF.CountryID AND FactDTF.DateKey = Dim_Date.DateKey
    LEFT JOIN
        FactUNStats ON DimCountry.CountryID = FactUNStats.CountryID AND FactUNStats.BaseYearDatekey = Dim_Date.DateKey AND FactUNStats.CreationDatekey = Dim_Date.DateKey;
    
"""
# query2= """SELECT TOP 5 FactUNStats.FactID AS UNStatsFactID,
#         FactUNStats.CountryID AS UNStatsCountryID,
#         FactUNStats.Value AS UNStatsValue,
#         FactUNStats.SNA93TableCode,
#         FactUNStats.SubGroup,
#         FactUNStats.Item AS UNStatsItem,
#         FactUNStats.SNA93ItemCode,
#         FactUNStats.Series AS UNStatsSeries,
#         FactUNStats.Currency AS UNStatsCurrency,
#         FactUNStats.SNASystem,
#         FactUNStats.FiscalYearType,
#         FactUNStats.ValueFootnotes,
#         FactUNStats.ETLDate AS UNStatsETLDate,
#         FactUNStats.BaseYearDatekey AS UNStatsBaseYearDatekey,
#         FactUNStats.CreationDatekey AS UNStatsCreationDatekeyLEFT JOIN 
#         FactUNStats ON DimCountry.CountryID = FactUNStats.CountryID""""
# Execute the query
cursor.execute(query)
batch_size = 5

# Initialize batch counter
batch_counter = 1

# Fetch the results
results = cursor.fetchmany(batch_size)

# Define the output directory




# Loop through the results in batches
while True:
    # Fetch the next batch of results
    results = cursor.fetchmany(batch_size)

    # If no more results, break the loop
    if not results:
        break

    # Initialize sentences list for the current batch
    sentences = []


    # Generate sentences based on the query results
    for result in results:
        # Combine column names and values into a dictionary
        result_dict = {column[0]: value for column, value in zip(cursor.description, result)}

        # Extract values from the dictionary
        country = result_dict['Country']
        region = result_dict['Region']
        subregion = result_dict['Subregion']
        capital_city = result_dict['CapitalCity']
        country_code = result_dict['Countrycode']
        iso_code = result_dict['IsoCode']
        longitude = result_dict['Longitude']
        latitude = result_dict['Latitude']
        continent = result_dict['Continent']
        start_date = result_dict['CountryStartDate']
        end_date = result_dict['CountryEndDate']
        country_key = result_dict['countryKey']
        income_level_value = result_dict['IncomeLevelValue']
        lending_type_value = result_dict['LendingTypeValue']
        etl_date_country = result_dict['CountryETLDate']
        country_region_code = result_dict['CountryRegionCode']
        currency_arabic = result_dict['CurrencyArabic']
        currency = result_dict['Currency']
        ioc_country_code = result_dict['IOCCountryCode']
        legacy_code = result_dict['Legacy_Code']
        reporter_country_id = result_dict['ReporterCountryId']
        partner_country_id = result_dict['PartnerCountryId']
        product_id = result_dict['ProductID']
        creation_date = result_dict['CreationDateKey']
        import_value = result_dict['Import_Value']
        export_value = result_dict['Export_Value']
        commodity_id = result_dict['Commodity_ID']
        classification = result_dict['Classification']
        creation_date_trade = result_dict['CREATIONDATE']
        trade_flow_code = result_dict['Trade Flow Code']
        trade_flow = result_dict['Trade Flow']
        qty_unit_code = result_dict['Qty Unit Code']
        qty_unit = result_dict['Qty Unit']
        qty = result_dict['Qty']
        alt_qty_unit_code = result_dict['Alt Qty Unit Code']
        alt_qty_unit = result_dict['Alt Qty Unit']
        alt_qty = result_dict['Alt Qty']
        netweight_kg = result_dict['Netweight (kg)']
        gross_weight_kg = result_dict['Gross weight (kg)']
        trade_value_usd = result_dict['Trade Value (US$)']
        cif_trade_value_usd = result_dict['CIF Trade Value (US$)']
        fob_trade_value_usd = result_dict['FOB Trade Value (US$)']
        flag = result_dict['Flag']
        is_active = result_dict['Is_Active']
        snapshot_date = result_dict['Snapshot_Date']
        is_mirror = result_dict['Is_Mirror']
        landedcost_export = result_dict['Landedcost_Export']
        landedcost_import = result_dict['Landedcost_Import']
        is_month_aggregate = result_dict['Is_MonthAggregate']
        year = result_dict['Year']
        item_id = result_dict['ItemId']
        hs02 = result_dict['HS02']
        hs02_description = result_dict['HS02Description']
        hs04 = result_dict['HS04']
        hs04_description = result_dict['HS04Description']
        hs06 = result_dict['HS06']
        hs06_description = result_dict['HS06Description']
        start_date_product = result_dict['ProductStartDate']
        end_date_product = result_dict['ProductEndDate']
        hs_source = result_dict['HSSource']
        major_category = result_dict['MajorCategory']
        major_category_description = result_dict['MajorCategoryDescription']
        isic_sk = result_dict['ISIC_SK']
        industry = result_dict['Industry']
        sub_industry = result_dict['Sub-Industry']
        creative_goods = result_dict['CreativeGoods']
        high_tech_goods = result_dict['HighTechGoods']
        industry_id_sk = result_dict['IndustryID_SK']
        product_description_arabic = result_dict['ProductDescriptionArabic']
        hs04_product_description_arabic = result_dict['HS04ProductDescriptionArabic']
        hs02_product_description_arabic = result_dict['HS02ProductDescriptionArabic']
        fact_id_pci = result_dict['FactId']
        rank_pci = result_dict['Rank']
        product_category_id_pci = result_dict['ProductCategoryID']
        date_key_pci = result_dict['DateKey']
        pci_value = result_dict['PCI Value']
        delta_pci = result_dict['Delta']
        product_name_pci = result_dict['ProductName']
        hs_version_pci = result_dict['HSVersion']
        hs_classification_pci = result_dict['HSClassification']
        fact_sk_world_bank = result_dict['FactSK']
        date_key_world_bank = result_dict['WorldBankDateKey']
        indicator_id_world_bank = result_dict['IndicatorId']
        indicator_value_world_bank = result_dict['IndicatorValue']
        value_world_bank = result_dict['Value']
        decimal_world_bank = result_dict['Decimal']
        etl_date_world_bank = result_dict['WorldBankETLDate']
        legacy_id_world_bank = result_dict['LegacyID']
        ksa_currency_rate_world_bank = result_dict['KSACurrencyRate']
        country_id1_desta = result_dict['CountryID1_SK']
        country_id2_desta = result_dict['CountryID2_SK']
        date_key_desta = result_dict['DestaDateKey']
        area_desta = result_dict['Area']
        sub_area_desta = result_dict['SubArea']
        name_desta = result_dict['Name']
        variables_desta = result_dict['Variables']
        variables_description_desta = result_dict['Variables_Description']
        values_description_desta = result_dict['Values_Description']
        fact_id_imf = result_dict['IMF_FactID']
        country_id_imf = result_dict['IMF_CountryID']
        indicator_code_imf = result_dict['IMF_IndicatorCode']
        indicator_name_imf = result_dict['IMF_IndicatorName']
        unit_imf = result_dict['IMF_Unit']
        value_imf = result_dict['IMF_Value']
        date_key_imf = result_dict['IMF_Datekey']
        rank = result_dict.get('Rank', 'N/A')  # Add variable for Rank
        product_name = result_dict.get('ProductName', 'N/A')  # Add variable for Product Name
        hs_version = result_dict.get('HSVersion', 'N/A')  # Add variable for HS Version
        hs_classification = result_dict.get('HSClassification', 'N/A')  # Add variable for HS Classification
        world_bank_indicator_value = result_dict.get('IndicatorValue', 'N/A')  # Add variable for World Bank Indicator Value
        world_bank_indicator_id = result_dict.get('IndicatorId', 'N/A')  # Add variable for World Bank Indicator ID
        world_bank_value = result_dict.get('Value', 'N/A')  # Add variable for World Bank Value
        world_bank_date_key = result_dict.get('WorldBankDateKey', 'N/A')  # Add variable for World Bank Date Key
        desta_variables_description = result_dict.get('Variables_Description', 'N/A')  # Add variable for Desta Variables Description
        desta_area = result_dict.get('Area', 'N/A')  # Add variable for Desta Area
        imf_indicator_name = result_dict.get('IMF_IndicatorName', 'N/A')  # Add variable for IMF Indicator Name
        imf_value = result_dict.get('IMF_Value', 'N/A')  # Add variable for IMF Value
        imf_date_key = result_dict.get('IMF_Datekey', 'N/A')  # Add variable for IMF Date Key
        # Extract values from the dictionary
        eci_fact_id = result_dict.get('ECIFactId', 'N/A')  # Add variable for FactECI FactId
        eci_rank = result_dict.get('ECIRank', 'N/A')  # Add variable for FactECI Rank
        eci_country_id = result_dict.get('ECICountryID', 'N/A')  # Add variable for FactECI CountryID
        eci_date_key = result_dict.get('ECIDateKey', 'N/A')  # Add variable for FactECI DateKey
        eci_value = result_dict.get('ECI Value', 'N/A')  # Add variable for FactECI ECI Value
        eci_delta = result_dict.get('ECIDelta', 'N/A')  # Add variable for FactECI Delta
        jodi_fact_id = result_dict.get('JodiFactID', 'N/A')  # Add variable for FactJodi FactID
        jodi_country_id = result_dict.get('JodiCountryID', 'N/A')  # Add variable for FactJodi CountryID
        jodi_date_key = result_dict.get('JodiDateKey', 'N/A')  # Add variable for FactJodi DateKey
        jodi_product = result_dict.get('JodiProduct', 'N/A')  # Add variable for FactJodi Product
        jodi_unit = result_dict.get('JodiUnit', 'N/A')  # Add variable for FactJodi Unit
        jodi_value = result_dict.get('JodiValue', 'N/A')  # Add variable for FactJodi Value
        unstats_fact_id = result_dict.get('UNStatsFactID', 'N/A')  # Add variable for FactUNStats FactID
        unstats_country_id = result_dict.get('UNStatsCountryID', 'N/A')  # Add variable for FactUNStats CountryID
        unstats_value = result_dict.get('UNStatsValue', 'N/A')  # Add variable for FactUNStats Value
        unstats_item = result_dict.get('UNStatsItem', 'N/A')  # Add variable for FactUNStats Item
        unstats_series = result_dict.get('UNStatsSeries', 'N/A')  # Add variable for FactUNStats Series
        unstats_currency = result_dict.get('UNStatsCurrency', 'N/A')  # Add variable for FactUNStats Currency
        unstats_etl_date = result_dict.get('UNStatsETLDate', 'N/A')  # Add variable for FactUNStats ETLDate
        dtf_fact_id = result_dict.get('FactId', 'N/A')
        dtf_date_key = result_dict.get('DateKey', 'N/A')
        dtf_country_id = result_dict.get('CountryID', 'N/A')
        ease_of_business_rank = result_dict.get('Ease of Doing Business Rank', 'N/A')
        overall_dtf = result_dict.get('Overall DTF', 'N/A')
        starting_business_rank = result_dict.get('Starting a Business - Rank', 'N/A')
        starting_business_dtf = result_dict.get('Starting a Business - DTF', 'N/A')
        starting_business_procedure_men_number = result_dict.get('Starting a Business - Procedure – Men (number)', 'N/A')
        starting_business_time_men_days = result_dict.get('Starting a Business - Time – Men (days)', 'N/A')
        starting_business_cost_men_percent_income = result_dict.get('Starting a Business - Cost – Men (% of income per capita)', 'N/A')
        starting_business_procedure_women_number = result_dict.get('Starting a Business - Procedure – Women (number)', 'N/A')
        starting_business_time_women_days = result_dict.get('Starting a Business - Time – Women (days)', 'N/A')
        starting_business_cost_women_percent_income = result_dict.get('Starting a Business - Cost – Women (% of income per capita)', 'N/A')
        starting_business_paid_in_min_capital_percent_income = result_dict.get('Starting a Business - Paid-in min. capital (% of income per capita)', 'N/A')
        dealing_construction_permits_rank = result_dict.get('Dealing with Construction Permits - Rank', 'N/A')
        dealing_construction_permits_dtf = result_dict.get('Dealing with Construction Permits - DTF', 'N/A')
        dealing_construction_permits_procedures_number = result_dict.get('Dealing with Construction Permits - Procedures (number)', 'N/A')
        dealing_construction_permits_time_days = result_dict.get('Dealing with Construction Permits - Time (days)', 'N/A')
        dealing_construction_permits_cost_percent_warehouse_value = result_dict.get('Dealing with Construction Permits - Cost (% of warehouse value)', 'N/A')
        dealing_construction_permits_building_quality_control_index = result_dict.get('Dealing with Construction Permits - Building quality control index (0-15)', 'N/A')
        dtf_building_regulations_index = result_dict.get('Dealing with Construction Permits - Quality of building regulations index (0-2)', 'N/A')
        dtf_quality_control_before_construction = result_dict.get('Dealing with Construction Permits - Quality control before construction index (0-1)', 'N/A')
        dtf_quality_control_during_construction = result_dict.get('Dealing with Construction Permits - Quality control during construction index (0-3)', 'N/A')
        dtf_quality_control_after_construction = result_dict.get('Dealing with Construction Permits - Quality control after construction index (0-3)', 'N/A')
        dtf_liability_and_insurance_index = result_dict.get('Dealing with Construction Permits - Liability and insurance regimes index (0-2)', 'N/A')
        dtf_professional_certifications_index = result_dict.get('Dealing with Construction Permits - Professional certifications index (0-4)', 'N/A')
        dtf_electricity_rank = result_dict.get('Getting Electricity - Rank', 'N/A')
        dtf_electricity_dtf = result_dict.get('Getting Electricity - DTF', 'N/A')
        dtf_electricity_procedures = result_dict.get('Getting Electricity - Procedures (number)', 'N/A')
        dtf_electricity_time_days = result_dict.get('Getting Electricity - Time (days)', 'N/A')
        dtf_electricity_cost_percent = result_dict.get('Getting Electricity - Cost (% of income per capita)', 'N/A')
        dtf_electricity_reliability_index = result_dict.get('Getting Electricity - Reliability of supply and transparency of tariff index (0-8)', 'N/A')
        dtf_electricity_outage_duration_frequency = result_dict.get('Getting Electricity - Total duration and frequency of outages per customer a year (0-3)', 'N/A')
        dtf_electricity_monitoring_outages = result_dict.get('Getting Electricity - Mechanisms for monitoring outages (0-1)', 'N/A')
        dtf_electricity_restoring_service = result_dict.get('Getting Electricity - Mechanisms for restoring service (0-1)', 'N/A')
        electricity_regulatory_monitoring = result_dict['ElectricityRegulatoryMonitoring']
        electricity_financial_deterrents = result_dict['ElectricityFinancialDeterrents']
        electricity_communication_tariffs = result_dict['ElectricityCommunicationTariffs']
        electricity_price = result_dict['ElectricityPrice']
        registering_property_rank = result_dict['RegisteringPropertyRank']
        registering_property_dtf = result_dict['RegisteringPropertyDTF']
        registering_property_procedures = result_dict['RegisteringPropertyProcedures']
        registering_property_time = result_dict['RegisteringPropertyTime']
        registering_property_cost = result_dict['RegisteringPropertyCost']
        registering_property_quality = result_dict['RegisteringPropertyQuality']
        registering_property_reliability = result_dict['RegisteringPropertyReliability']
        registering_property_transparency = result_dict['RegisteringPropertyTransparency']
        registering_property_geographic_coverage = result_dict['RegisteringPropertyGeographicCoverage']
        registering_property_land_dispute_resolution = result_dict['RegisteringPropertyLandDisputeResolution']
        registering_property_equal_access = result_dict['RegisteringPropertyEqualAccess']
        getting_credit_rank = result_dict['GettingCreditRank']
        getting_credit_dtf = result_dict['GettingCreditDTF']
        getting_credit_strength_legal_rights = result_dict['GettingCreditStrengthLegalRights']
        getting_credit_depth_credit_information = result_dict['GettingCreditDepthCreditInformation']
        getting_credit_registry_coverage = result_dict['GettingCreditCreditRegistryCoverage']
        getting_credit_bureau_coverage = result_dict['GettingCreditCreditBureauCoverage']
        protecting_minority_rank = result_dict.get('Protecting Minority Investors - Rank', 'N/A')
        protecting_minority_dtf = result_dict.get('Protecting Minority Investors - DTF', 'N/A')
        conflict_of_interest_index = result_dict.get('Protecting Minority Investors - Extent of conflict of interest regulation index (0-10)', 'N/A')
        minority_protection_strength_index = result_dict.get('Protecting Minority Investors - Strength of minority investor protection index (0-10)', 'N/A')
        disclosure_index = result_dict.get('Protecting Minority Investors - Extent of disclosure index (0-10)', 'N/A')
        director_liability_index = result_dict.get('Protecting Minority Investors - Extent of director liability index (0-10)', 'N/A')
        shareholder_suits_index = result_dict.get('Protecting Minority Investors - Ease of shareholder suits index (0-10)', 'N/A')
        old_shareholder_suits_index = result_dict.get('Protecting Minority Investors - Ease of shareholder suits index (0-10) old methodology', 'N/A')
        shareholder_governance_index = result_dict.get('Protecting Minority Investors - Extent of shareholder governance index (0-10)', 'N/A')
        shareholder_rights_index = result_dict.get('Protecting Minority Investors - Extent of shareholder rights index (0-10)', 'N/A')
        ownership_control_index = result_dict.get('Protecting Minority Investors - Extent of ownership and control index (0-10)', 'N/A')
        corporate_transparency_index = result_dict.get('Protecting Minority Investors - Extent of corporate transparency index (0-10)', 'N/A')
        paying_taxes_rank = result_dict.get('Paying Taxes - Rank', 'N/A')
        paying_taxes_dtf = result_dict.get('Paying Taxes - DTF', 'N/A')
        postfiling_index = result_dict.get('Paying Taxes - Postfiling index (0-100)', 'N/A')
        payments_per_year = result_dict.get('Paying Taxes - Payments (number per year)', 'N/A')
        dtf_enforcing_contracts = result_dict.get('Enforcing Contracts - DTF', 'N/A')
        dtf_enforcing_contracts_time = result_dict.get('Enforcing Contracts - Time (days)', 'N/A')
        dtf_enforcing_contracts_cost = result_dict.get('Enforcing Contracts - Cost (% of claim value)', 'N/A')
        dtf_enforcing_contracts_quality = result_dict.get('Enforcing Contracts - Quality of judicial processes index (0-18)', 'N/A')
        dtf_resolving_insolvency_rank = result_dict.get('Resolving Insolvency - Rank', 'N/A')
        dtf_resolving_insolvency_dtf = result_dict.get('Resolving Insolvency - DTF', 'N/A')
        dtf_resolving_insolvency_recovery_rate = result_dict.get('Resolving Insolvency - Recovery rate (cents on the dollar)', 'N/A')
        dtf_resolving_insolvency_time = result_dict.get('Resolving Insolvency - Time (years)', 'N/A')
        dtf_resolving_insolvency_cost = result_dict.get('Resolving Insolvency - Cost (% of estate)', 'N/A')
        dtf_resolving_insolvency_outcome = result_dict.get('Resolving Insolvency - Outcome (0 as piecemeal sale and 1 as going concern)', 'N/A')
        dtf_resolving_insolvency_strength = result_dict.get('Resolving Insolvency - Strength of insolvency framework index (0-16)', 'N/A')
        dtf_resolving_insolvency_commencement = result_dict.get('Resolving Insolvency - Commencement of proceedings index (0-3)', 'N/A')
        dtf_resolving_insolvency_management = result_dict.get("Resolving Insolvency - Management of debtor's assets index (0-6)", 'N/A')
        dtf_resolving_insolvency_reorganization = result_dict.get('Resolving Insolvency - Reorganization proceedings index (0-3)', 'N/A')
        dtf_resolving_insolvency_creditor_participation = result_dict.get('Resolving Insolvency - Creditor participation index (0-4)', 'N/A')
        dtf_region = result_dict.get('Region', 'N/A')
        dtf_income_group = result_dict.get('Income Group', 'N/A')
        date_key = result_dict.get('DateKey', 'N/A')
        full_date = result_dict.get('FullDate', 'N/A')
        full_date_ar = result_dict.get('FullDate_Ar', 'N/A')
        date_name = result_dict.get('DateName', 'N/A')
        date_name_us = result_dict.get('DateNameUS', 'N/A')
        date_name_eu = result_dict.get('DateNameEU', 'N/A')
        day_of_week = result_dict.get('DayOfWeek', 'N/A')
        day_name_of_week = result_dict.get('DayNameOfWeek', 'N/A')
        day_name_of_week_ar = result_dict.get('DayNameOfWeek_Ar', 'N/A')
        day_of_month = result_dict.get('DayOfMonth', 'N/A')
        day_of_year = result_dict.get('DayOfYear', 'N/A')
        weekday_weekend = result_dict.get('WeekdayWeekend', 'N/A')
        week_of_year = result_dict.get('WeekOfYear', 'N/A')
        month_name = result_dict.get('MonthName', 'N/A')
        month_name_ar = result_dict.get('MonthName_Ar', 'N/A')
        month_of_year = result_dict.get('MonthOfYear', 'N/A')
        is_last_day_of_month = result_dict.get('IsLastDayOfMonth', 'N/A')
        calendar_quarter = result_dict.get('CalendarQuarter', 'N/A')
        calendar_year = result_dict.get('CalendarYear', 'N/A')
        calendar_year_month = result_dict.get('CalendarYearMonth', 'N/A')
        calendar_year_qtr = result_dict.get('CalendarYearQtr', 'N/A')
        fiscal_month_of_year = result_dict.get('FiscalMonthOfYear', 'N/A')
        fiscal_quarter = result_dict.get('FiscalQuarter', 'N/A')
        fiscal_year = result_dict.get('FiscalYear', 'N/A')
        fiscal_year_month = result_dict.get('FiscalYearMonth', 'N/A')
        fiscal_year_qtr = result_dict.get('FiscalYearQtr', 'N/A')
        audit_key = result_dict.get('AuditKey', 'N/A')

        # Create a sentence
        sentence = (
            f"In {country}, we observed an import value of {import_value} and an export value of {export_value}. "
            f"The product with creation date {creation_date} follows the trade flow of {trade_flow}. "
            f"The net weight is approximately {netweight_kg} kilograms, and the gross weight is {gross_weight_kg} kilograms. "
            f"The country is located in {region}, {subregion}, with a capital city of {capital_city}. "
            f"ISO Code: {iso_code}, Longitude: {longitude}, Latitude: {latitude}, Continent: {continent}. "
            f"The country falls under {country_region_code} region and is categorized as {income_level_value} income level. "
            f"The lending type is {lending_type_value}, and the data was last updated on . "
            f"The product falls under the {industry} industry and {sub_industry} sub-industry. "
            f"It is categorized as {major_category} in the {hs02_description} classification. "
            f"The product has a description in Arabic: {product_description_arabic}. "
            f"The PCI value for this product is {pci_value}, with a rank of {rank}. "
            f"The product is named {product_name} and falls under HS version {hs_version} with classification {hs_classification}. "
            f"World Bank data indicates {world_bank_indicator_value} for the indicator with ID {world_bank_indicator_id}. "
            f"The value is {world_bank_value}, recorded on {world_bank_date_key}. "
            f"FactDesta data indicates {desta_variables_description} in the {desta_area} area and {sub_area_desta} sub-area. "
            f"IMF data reports {imf_indicator_name} with a value of {imf_value} on {imf_value}."
            f"The Economic Complexity Index (ECI) for the country is {eci_value}, with a rank of {eci_rank}. "
            f"The ECI data was recorded on {eci_date_key}."
            f"Joint Organisations Data Initiative (JODI) data indicates {jodi_product} with a value of {jodi_value} in {jodi_unit}. "
            f"The Joint Organisations Data Initiative (JODI) data was recorded on {jodi_date_key}."
            f"The Jodi data indicates {jodi_product} with a value of {jodi_value} in {jodi_unit}. "
            f"The Jodi data was recorded on {jodi_date_key}. "
            f"The United Nations Statistics (UNStats) data shows {unstats_item} in {unstats_currency} with a value of {unstats_value}. "
            f"The UNStats data was recorded on {unstats_etl_date}."
            
        )
        dtf_fact_id_sentence = f"For {country}, in the Ease of Doing Business data (FactDTF), the Fact ID is {dtf_fact_id}."
        dtf_date_key_sentence = f"For {country}, in the Ease of Doing Business data (FactDTF), the Date Key is {dtf_date_key}."
        dtf_country_id_sentence = f"For {country}, in the Ease of Doing Business data (FactDTF), the Country ID is {dtf_country_id}."
        ease_of_business_rank_sentence = f"For {country}, in the Ease of Doing Business data (FactDTF), the Ease of Business Rank is {ease_of_business_rank}."
        overall_dtf_sentence = f"For {country}, in the Ease of Doing Business data (FactDTF), the Overall DTF is {overall_dtf}."
        starting_business_rank_sentence = f"For {country}, in the Ease of Doing Business data (FactDTF), the Starting Business Rank is {starting_business_rank}."
        starting_business_dtf_sentence = f"For {country}, in the Ease of Doing Business data (FactDTF), the Starting Business DTF is {starting_business_dtf}."
        starting_business_procedure_men_number_sentence = f"For {country}, in the Ease of Doing Business data (FactDTF), the Starting Business Procedure for Men (Number) is {starting_business_procedure_men_number}."
        starting_business_time_men_days_sentence = f"For {country}, in the Ease of Doing Business data (FactDTF), the Starting Business Time for Men (Days) is {starting_business_time_men_days}."
        starting_business_cost_men_percent_income_sentence = f"For {country}, in the Ease of Doing Business data (FactDTF), the Starting Business Cost for Men (% of Income) is {starting_business_cost_men_percent_income}."
        starting_business_procedure_women_number_sentence = f"For {country}, in the Ease of Doing Business data (FactDTF), the Starting Business Procedure for Women (Number) is {starting_business_procedure_women_number}."
        starting_business_time_women_days_sentence = f"For {country}, in the Ease of Doing Business data (FactDTF), the Starting Business Time for Women (Days) is {starting_business_time_women_days}."
        starting_business_cost_women_percent_income_sentence = f"For {country}, in the Ease of Doing Business data (FactDTF), the Starting Business Cost for Women (% of Income) is {starting_business_cost_women_percent_income}."
        starting_business_paid_in_min_capital_percent_income_sentence = f"For {country}, in the Ease of Doing Business data (FactDTF), the Starting Business Paid-in Min. Capital (% of Income) is {starting_business_paid_in_min_capital_percent_income}."
        dealing_construction_permits_rank_sentence = f"For {country}, in the Ease of Doing Business data (FactDTF), the Dealing with Construction Permits Rank is {dealing_construction_permits_rank}."
        dealing_construction_permits_dtf_sentence = f"For {country}, in the Ease of Doing Business data (FactDTF), the Dealing with Construction Permits DTF is {dealing_construction_permits_dtf}."
        dealing_construction_permits_procedures_number_sentence = f"For {country}, in the Ease of Doing Business data (FactDTF), the Dealing with Construction Permits Procedures (Number) is {dealing_construction_permits_procedures_number}."
        dealing_construction_permits_time_days_sentence = f"For {country}, in the Ease of Doing Business data (FactDTF), the Dealing with Construction Permits Time (Days) is {dealing_construction_permits_time_days}."
        dealing_construction_permits_cost_percent_warehouse_value_sentence = f"For {country}, in the Ease of Doing Business data (FactDTF), the Dealing with Construction Permits Cost (% of Warehouse Value) is {dealing_construction_permits_cost_percent_warehouse_value}."
        dealing_construction_permits_building_quality_control_index_sentence = f"For {country}, in the Ease of Doing Business data (FactDTF), the Dealing with Construction Permits Building Quality Control Index (0-15) is {dealing_construction_permits_building_quality_control_index}."
        dtf_sentences = [
            dtf_fact_id_sentence,
            dtf_date_key_sentence,
            dtf_country_id_sentence,
            ease_of_business_rank_sentence,
            overall_dtf_sentence,
            starting_business_rank_sentence,
            starting_business_dtf_sentence,
            starting_business_procedure_men_number_sentence,
            starting_business_time_men_days_sentence,
            starting_business_cost_men_percent_income_sentence,
            starting_business_procedure_women_number_sentence,
            starting_business_time_women_days_sentence,
            starting_business_cost_women_percent_income_sentence,
            starting_business_paid_in_min_capital_percent_income_sentence,
            dealing_construction_permits_rank_sentence,
            dealing_construction_permits_dtf_sentence,
            dealing_construction_permits_procedures_number_sentence,
            dealing_construction_permits_time_days_sentence,
            dealing_construction_permits_cost_percent_warehouse_value_sentence,
            dealing_construction_permits_building_quality_control_index_sentence,
            # ... (Include all other sentences for FactDTF)
        ]
        # Append the sentence to the list
        
        result_string = (
            dtf_fact_id_sentence +
            dtf_date_key_sentence +
            dtf_country_id_sentence +
            ease_of_business_rank_sentence +
            overall_dtf_sentence +
            starting_business_rank_sentence +
            starting_business_dtf_sentence +
            starting_business_procedure_men_number_sentence +
            starting_business_time_men_days_sentence +
            starting_business_cost_men_percent_income_sentence +
            starting_business_procedure_women_number_sentence +
            starting_business_time_women_days_sentence +
            starting_business_cost_women_percent_income_sentence +
            starting_business_paid_in_min_capital_percent_income_sentence +
            dealing_construction_permits_rank_sentence +
            dealing_construction_permits_dtf_sentence +
            dealing_construction_permits_procedures_number_sentence +
            dealing_construction_permits_time_days_sentence +
            dealing_construction_permits_cost_percent_warehouse_value_sentence +
            dealing_construction_permits_building_quality_control_index_sentence
        )
        sentence2 = (
            f"The Doing Business data shows that this country scores {dtf_building_regulations_index} in the Quality of Building Regulations Index, "
            f"{dtf_quality_control_before_construction} in Quality Control Before Construction Index, "
            f"{dtf_quality_control_during_construction} in Quality Control During Construction Index, "
            f"{dtf_quality_control_after_construction} in Quality Control After Construction Index, "
            f"{dtf_liability_and_insurance_index} in Liability and Insurance Regimes Index, and "
            f"{dtf_professional_certifications_index} in Professional Certifications Index. "
            f"In the Getting Electricity category, the country has a rank of {dtf_electricity_rank}, "
            f"DTF of {dtf_electricity_dtf}, {dtf_electricity_procedures} procedures, {dtf_electricity_time_days} days duration, "
            f"{dtf_electricity_cost_percent}% cost of income per capita, "
            f"{dtf_electricity_reliability_index} in Reliability of Supply and Transparency of Tariff Index, "
            f"{dtf_electricity_outage_duration_frequency} in Total Duration and Frequency of Outages per Customer a Year, "
            f"{dtf_electricity_monitoring_outages} in Mechanisms for Monitoring Outages, and "
            f"{dtf_electricity_restoring_service} in Mechanisms for Restoring Service."
        )
        sentence_dtf = (
            f"In {country}, the Electricity Regulatory Monitoring is {electricity_regulatory_monitoring}. "
            f"The Financial Deterrents aimed at limiting outages is {electricity_financial_deterrents}. "
            f"The Communication of tariffs and tariff changes is {electricity_communication_tariffs}. "
            f"The Price of electricity is {electricity_price} US cents per kWh. "
            f"The Registering Property Rank is {registering_property_rank}, and the DTF is {registering_property_dtf}. "
            f"The number of procedures for Registering Property is {registering_property_procedures}. "
            f"The time required for Registering Property is {registering_property_time} days. "
            f"The cost of Registering Property is {registering_property_cost}% of property value. "
            f"The Quality of the land administration index is {registering_property_quality} (0-30). "
            f"The Reliability of infrastructure index is {registering_property_reliability} (0-8). "
            f"The Transparency of information index is {registering_property_transparency} (0–6). "
            f"The Geographic coverage index is {registering_property_geographic_coverage} (0–8). "
            f"The Land dispute resolution index is {registering_property_land_dispute_resolution} (0–8). "
            f"The Equal access to property rights index is {registering_property_equal_access} (-2–0). "
            f"The Getting Credit Rank is {getting_credit_rank}, and the DTF is {getting_credit_dtf}. "
            f"The Strength of legal rights index is {getting_credit_strength_legal_rights} (0-12). "
            f"The Depth of credit information index is {getting_credit_depth_credit_information} (0-8). "
            f"The Credit registry coverage is {getting_credit_registry_coverage}% of adults. "
            f"The Credit bureau coverage is {getting_credit_bureau_coverage}% of adults."
        )
        protecting_minority_sentence = (
            f"In {country}, the Protecting Minority Investors rank is {protecting_minority_rank}, and the DTF is {protecting_minority_dtf}. "
            f"The extent of conflict of interest regulation index is {conflict_of_interest_index}. "
            f"The strength of minority investor protection index is {minority_protection_strength_index}. "
            f"The extent of disclosure index is {disclosure_index}."
        )

        shareholder_suits_sentence = (
            f"In {country}, the ease of shareholder suits index is {shareholder_suits_index}, and the old methodology index is {old_shareholder_suits_index}. "
            f"The extent of shareholder governance index is {shareholder_governance_index}. "
            f"The extent of shareholder rights index is {shareholder_rights_index}."
        )

        ownership_control_sentence = (
            f"In {country}, the extent of ownership and control index is {ownership_control_index}. "
            f"The extent of corporate transparency index is {corporate_transparency_index}."
        )

        paying_taxes_sentence = (
            f"In {country}, the Paying Taxes rank is {paying_taxes_rank}, and the DTF is {paying_taxes_dtf}. "
            f"The postfiling index is {postfiling_index}, and there are {payments_per_year} payments per year."
        )
        dtf_enforcing_contracts_sentence = f"The DTF data indicates an 'Enforcing Contracts - DTF' value of {dtf_enforcing_contracts}."
        dtf_enforcing_contracts_time_sentence = f"The DTF data shows that 'Enforcing Contracts - Time (days)' is approximately {dtf_enforcing_contracts_time} days."
        dtf_enforcing_contracts_cost_sentence = f"The DTF data reveals that the cost of enforcing contracts is '{dtf_enforcing_contracts_cost}%' of the claim value."
        dtf_enforcing_contracts_quality_sentence = f"The DTF data rates the 'Quality of judicial processes index (0-18)' at {dtf_enforcing_contracts_quality}."
        dtf_resolving_insolvency_rank_sentence = f"In terms of resolving insolvency, the country is ranked at {dtf_resolving_insolvency_rank}."
        dtf_resolving_insolvency_dtf_sentence = f"The DTF value for resolving insolvency is {dtf_resolving_insolvency_dtf}."
        dtf_resolving_insolvency_recovery_rate_sentence = f"The recovery rate in resolving insolvency is {dtf_resolving_insolvency_recovery_rate} cents on the dollar."
        dtf_resolving_insolvency_time_sentence = f"The time required for resolving insolvency is approximately {dtf_resolving_insolvency_time} years."
        dtf_resolving_insolvency_cost_sentence = f"The cost of resolving insolvency is '{dtf_resolving_insolvency_cost}%' of the estate."
        dtf_resolving_insolvency_outcome_sentence = f"The outcome of resolving insolvency is categorized as '{dtf_resolving_insolvency_outcome}'."
        dtf_resolving_insolvency_strength_sentence = f"The strength of the insolvency framework, according to DTF, is rated at {dtf_resolving_insolvency_strength}."
        dtf_resolving_insolvency_commencement_sentence = f"The commencement of proceedings index for resolving insolvency is {dtf_resolving_insolvency_commencement}."
        dtf_resolving_insolvency_management_sentence = f"The management of debtor's assets index for resolving insolvency is {dtf_resolving_insolvency_management}."
        dtf_resolving_insolvency_reorganization_sentence = f"The reorganization proceedings index for resolving insolvency is {dtf_resolving_insolvency_reorganization}."
        dtf_resolving_insolvency_creditor_participation_sentence = f"The creditor participation index for resolving insolvency is {dtf_resolving_insolvency_creditor_participation}."
        dtf_region_sentence = f"The country falls under the '{dtf_region}' region according to DTF data."
        dtf_income_group_sentence = f"The income group classification according to DTF is '{dtf_income_group}'."





        date_key_sentence = f"The date key is {date_key}."
        full_date_sentence = f"The full date is {full_date}."
        full_date_ar_sentence = f"The full date in Arabic is {full_date_ar}."
        date_name_sentence = f"The date name is {date_name}."
        date_name_us_sentence = f"The date name in the US format is {date_name_us}."
        date_name_eu_sentence = f"The date name in the EU format is {date_name_eu}."
        day_of_week_sentence = f"The day of the week is {day_of_week}."
        day_name_of_week_sentence = f"The day name of the week is {day_name_of_week}."
        day_name_of_week_ar_sentence = f"The day name of the week in Arabic is {day_name_of_week_ar}."
        day_of_month_sentence = f"The day of the month is {day_of_month}."
        day_of_year_sentence = f"The day of the year is {day_of_year}."
        weekday_weekend_sentence = f"The day is classified as {weekday_weekend}."
        week_of_year_sentence = f"The week of the year is {week_of_year}."
        month_name_sentence = f"The month name is {month_name}."
        month_name_ar_sentence = f"The month name in Arabic is {month_name_ar}."
        month_of_year_sentence = f"The month of the year is {month_of_year}."
        is_last_day_of_month_sentence = f"It is {is_last_day_of_month} whether it's the last day of the month."
        calendar_quarter_sentence = f"The calendar quarter is {calendar_quarter}."
        calendar_year_sentence = f"The calendar year is {calendar_year}."
        calendar_year_month_sentence = f"The calendar year and month is {calendar_year_month}."
        calendar_year_qtr_sentence = f"The calendar year and quarter is {calendar_year_qtr}."
        fiscal_month_of_year_sentence = f"The fiscal month of the year is {fiscal_month_of_year}."
        fiscal_quarter_sentence = f"The fiscal quarter is {fiscal_quarter}."
        fiscal_year_sentence = f"The fiscal year is {fiscal_year}."
        fiscal_year_month_sentence = f"The fiscal year and month is {fiscal_year_month}."
        fiscal_year_qtr_sentence = f"The fiscal year and quarter is {fiscal_year_qtr}."
        audit_key_sentence = f"The audit key is {audit_key}."
# Append the sentences to the list
        


        sentence3 = (
            f"{dtf_enforcing_contracts_sentence} "
            f"{dtf_enforcing_contracts_time_sentence} "
            f"{dtf_enforcing_contracts_cost_sentence} "
            f"{dtf_enforcing_contracts_quality_sentence} "
            f"{dtf_resolving_insolvency_rank_sentence} "
            f"{dtf_resolving_insolvency_dtf_sentence} "
            f"{dtf_resolving_insolvency_recovery_rate_sentence} "
            f"{dtf_resolving_insolvency_time_sentence} "
            f"{dtf_resolving_insolvency_cost_sentence} "
            f"{dtf_resolving_insolvency_outcome_sentence} "
            f"{dtf_resolving_insolvency_strength_sentence} "
            f"{dtf_resolving_insolvency_commencement_sentence} "
            f"{dtf_resolving_insolvency_management_sentence} "
            f"{dtf_resolving_insolvency_reorganization_sentence} "
            f"{dtf_resolving_insolvency_creditor_participation_sentence} "
            f"{dtf_region_sentence} "
            f"{dtf_income_group_sentence}"
        )
        combined_sentence = date_key_sentence + full_date_sentence + full_date_ar_sentence + date_name_sentence + date_name_us_sentence + date_name_eu_sentence + day_of_week_sentence + day_name_of_week_sentence + day_name_of_week_ar_sentence + day_of_month_sentence + day_of_year_sentence + weekday_weekend_sentence + week_of_year_sentence + month_name_sentence + month_name_ar_sentence + month_of_year_sentence + is_last_day_of_month_sentence + calendar_quarter_sentence + calendar_year_sentence + calendar_year_month_sentence + calendar_year_qtr_sentence + fiscal_month_of_year_sentence + fiscal_quarter_sentence + fiscal_year_sentence + fiscal_year_month_sentence + fiscal_year_qtr_sentence + audit_key_sentence


        sentence += result_string + sentence2 + sentence_dtf+ protecting_minority_sentence + shareholder_suits_sentence + ownership_control_sentence + paying_taxes_sentence + sentence3 + combined_sentence
        sentences.append(sentence)
        sentences.extend([])
        
    # Write the batch to a text file
    output_directory = 'C:/Users/Nashit Budhwani/Desktop/Outputs/'
    file_path = f'output_batch_{batch_counter}.txt'
    output_directory += file_path

    with open(output_directory, 'w',encoding='utf-8') as file:
        file.write('\n\n\'.join(sentences))

    # Increment the batch counter
    batch_counter += 1

# Close the cursor and connection
cursor.close()
mssql_conn.close()
