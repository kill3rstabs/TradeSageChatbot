import pyodbc
import os

# Connect to MSSQL Server
mssql_conn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-RVRG19P;DATABASE=TradeForesightLLM;')

# Create a cursor object
cursor = mssql_conn.cursor()

query = """
    SELECT
        Comtrade_Services.[Id] AS [Comtrade_Services_Id],
        Comtrade_Services.[Commodity Code] AS [Comtrade_Services_Commodity_Code],
        Comtrade_Services.[Commodity] AS [Comtrade_Services_Commodity],
        Comtrade_EBOPS.[Id] AS [Comtrade_EBOPS_Id],
        Comtrade_EBOPS.[Classification],
        Comtrade_EBOPS.[Year],
        Comtrade_EBOPS.[Period],
        Comtrade_EBOPS.[Period Desc#] AS [Period_Desc],
        Comtrade_EBOPS.[Aggregate Level] AS [Aggregate_Level],
        Comtrade_EBOPS.[Trade Flow Code] AS [Trade_Flow_Code],
        Comtrade_EBOPS.[Trade Flow] AS [Trade_Flow],
        Comtrade_EBOPS.[Reporter Code] AS [Reporter_Code],
        Comtrade_EBOPS.[Reporter],
        Comtrade_EBOPS.[Reporter ISO] AS [Reporter_ISO],
        Comtrade_EBOPS.[Partner Code] AS [Partner_Code],
        Comtrade_EBOPS.[Partner],
        Comtrade_EBOPS.[Partner ISO] AS [Partner_ISO],
        Comtrade_EBOPS.[Commodity Code] AS [Commodity_Code_EBOPS],
        Comtrade_EBOPS.[Commodity] AS [Commodity_EBOPS],
        Comtrade_EBOPS.[Trade Value (US$)] AS [Trade_Value_USD]
    FROM
        Comtrade_Services
    INNER JOIN
        Comtrade_EBOPS ON Comtrade_Services.[Commodity Code] = Comtrade_EBOPS.[Commodity Code]
"""

cursor.execute(query)

# Fetch the results
results = cursor.fetchall()

# Define the output directory
output_directory = 'C:/Users/Nashit Budhwani/Desktop/'

# Define batch size
batch_size = 1600000

# Initialize batch counter
batch_counter = 1

# Loop through the results in batches
for i in range(0, len(results), batch_size):
    # Extract the current batch
    batch = results[i:i + batch_size]

    # Initialize sentences list for the current batch
    sentences = []

    # Generate sentences based on the query results
    for result in batch:
        # Combine column names and values into a dictionary
        result_dict = {column[0]: value for column, value in zip(cursor.description, result)}
        comtrade_services_id = result_dict.get('Comtrade_Services_Id', 'N/A')
        comtrade_services_commodity_code = result_dict.get('Comtrade_Services_Commodity_Code', 'N/A')
        comtrade_services_commodity = result_dict.get('Comtrade_Services_Commodity', 'N/A')

        # Extracting values for Comtrade_EBOPS
        comtrade_ebops_id = result_dict.get('Comtrade_EBOPS_Id', 'N/A')
        comtrade_ebops_classification = result_dict.get('Classification', 'N/A')
        comtrade_ebops_year = result_dict.get('Year', 'N/A')
        comtrade_ebops_period = result_dict.get('Period', 'N/A')
        comtrade_ebops_period_desc = result_dict.get('Period_Desc', 'N/A')
        comtrade_ebops_aggregate_level = result_dict.get('Aggregate_Level', 'N/A')
        comtrade_ebops_trade_flow_code = result_dict.get('Trade_Flow_Code', 'N/A')
        comtrade_ebops_trade_flow = result_dict.get('Trade_Flow', 'N/A')
        comtrade_ebops_reporter_code = result_dict.get('Reporter_Code', 'N/A')
        comtrade_ebops_reporter = result_dict.get('Reporter', 'N/A')
        comtrade_ebops_reporter_iso = result_dict.get('Reporter_ISO', 'N/A')
        comtrade_ebops_partner_code = result_dict.get('Partner_Code', 'N/A')
        comtrade_ebops_partner = result_dict.get('Partner', 'N/A')
        comtrade_ebops_partner_iso = result_dict.get('Partner_ISO', 'N/A')
        comtrade_ebops_commodity_code = result_dict.get('Commodity_Code_EBOPS', 'N/A')
        comtrade_ebops_commodity = result_dict.get('Commodity_EBOPS', 'N/A')
        comtrade_ebops_trade_value_usd = result_dict.get('Trade_Value_USD', 'N/A')

        comtrade_ebops_description = (
            f"The record with ID {comtrade_ebops_id} represents a trade entry in the Comtrade EBOPS table. "
            f"It belongs to the classification system {comtrade_ebops_classification}, indicating the trade data classification, such as SITC or HS. "
            f"The trade occurred in the year {comtrade_ebops_year} during the {comtrade_ebops_period_desc} period. "
            f"The trade involves an {comtrade_ebops_aggregate_level} level of aggregation. "
            f"The direction of trade flow is represented by the code {comtrade_ebops_trade_flow_code}, indicating {comtrade_ebops_trade_flow}. "
            f"The reporting country is {comtrade_ebops_reporter} ({comtrade_ebops_reporter_iso}), and the partner country is {comtrade_ebops_partner} ({comtrade_ebops_partner_iso}). "
            f"The commodity being traded has a code of {comtrade_ebops_commodity_code} and is described as {comtrade_ebops_commodity}. "
            f"The total trade value is {comtrade_ebops_trade_value_usd} US dollars. "
        )

        # Sentences for Comtrade_Services
        comtrade_services_description = (
            f"The record with ID {comtrade_services_id} represents a service entry in the Comtrade Services table. "
            f"The service is identified by the code {comtrade_services_commodity_code}, and it is described as {comtrade_services_commodity}."
        )
        sentence = comtrade_services_description + comtrade_ebops_description
        sentences.append(sentence)

    # Write the batch to a text file
    file_path = os.path.join(output_directory, f'output_batch_Commtrade_{batch_counter}.txt')
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write('\n\n\n'.join(sentences))

    # Increment the batch counter
    batch_counter += 1

# Close the cursor and connection
cursor.close()
mssql_conn.close()
