import pandas as pd
import os
import time

#start a timer to measure total elapsed time
script_start_time = time.time()

# Define the list of Product_or_Service Codes to save - https://www.acquisition.gov/sites/default/files/manual/PSC%20April%202024.xlsx
# BPS awards are in these codes (as of 09/30/2024) - "R499", "D399", "D306", "R408", "R410", "D308", "D318", "D301", "DC01", "DA01" 
# These codes are not on any award document but coded in FPDS.
# BPS awards - https://www.fpds.gov/ezsearch/search.do?indexName=awardfull&templateName=1.5.3&s=FPDS.GOV&q=%22Business+Performance+Systems%22
# Additional codes beyond those that BPS has are relevant because they are also IT related.  For example, BTTB BIA ITSS IDIQ's PSC code is DF01
# Including these codes expands the kind of opportunities that BPS can target 
psc_codes_to_filter = ["R499", "D399", "D306", "R408", "R410", "D308", "D318", "D301", "DC01", "DA01", "DF01", 
                       "D","D3","D301","D302","D303","D304","D305",
                       "D306","D307","D308","D309","D310","D311",
                       "D312","D313","D314","D315","D316","D317",
                       "D318","D319","D320","D321","D322","D324","D325","D399","DA","DA01","DA10",
                       "DB","DB01","DB02","DB10","DC","DC01","DC10","DD","DD01","DE","DE01","DE02",
                       "DE10","DE11","DF","DF01","DF10","DG","DG01","DG10","DG11","DH","DH01",
                       "DH10","DJ","DJ01","DJ10","DJ10","DK","DK01","DK10",
                       ]
# Convert the codes to a set for faster lookup
psc_codes_hash_set = set(psc_codes_to_filter)

# Define data types for columns based on the original list of field types
dtype_mapping = { 
    'contract_award_unique_key': 'str',
    'award_id_piid': 'str',
    'modification_number': 'str',
    'transaction_number': 'str',
    'federal_action_obligation': 'float',
    'total_dollars_obligated': 'float',
    'base_and_exercised_options_value': 'float',
    'current_total_value_of_award': 'float',
    'base_and_all_options_value': 'float',
    'potential_total_value_of_award': 'float',
    'action_date': 'str',  # Using str to handle various date formats
    'action_date_fiscal_year': 'Int64',
    'period_of_performance_start_date': 'str',
    'period_of_performance_current_end_date': 'str',
    'period_of_performance_potential_end_date': 'str',
    'ordering_period_end_date': 'str',
    'solicitation_date': 'str',
    'awarding_agency_name': 'str',
    'awarding_sub_agency_name': 'str',
    'awarding_office_name': 'str',
    'funding_agency_name': 'str',
    'funding_sub_agency_name': 'str',
    'funding_office_name': 'str',
    'recipient_uei': 'str',
    'recipient_name': 'str',
    'recipient_name_raw': 'str',
    'cage_code': 'str',
    'recipient_parent_uei': 'str',
    'recipient_parent_name': 'str',
    'recipient_parent_name_raw': 'str',
    'recipient_country_code': 'str',
    'recipient_address_line_1': 'str',
    'recipient_city_name': 'str',
    'recipient_county_name': 'str',
    'recipient_state_code': 'str',
    'recipient_state_name': 'str',
    'recipient_zip_4_code': 'str',
    'recipient_phone_number': 'str',
    'primary_place_of_performance_country_code': 'str',
    'primary_place_of_performance_city_name': 'str',
    'primary_place_of_performance_state_code': 'str',
    'award_or_idv_flag': 'str',
    'award_type': 'str',
    'type_of_contract_pricing': 'str',
    'transaction_description': 'str',
    'prime_award_base_transaction_description': 'str',
    'action_type': 'str',
    'solicitation_identifier': 'str',
    'number_of_actions': 'Int64',
    'inherently_governmental_functions_description': 'str',
    'product_or_service_code': 'str',
    'product_or_service_code_description': 'str',
    'contract_bundling': 'str',
    'dod_claimant_program_description': 'str',
    'naics_code': 'str',
    'naics_description': 'str',
    'information_technology_commercial_item_category': 'str',
    'subcontracting_plan': 'str',
    'extent_competed': 'str',
    'solicitation_procedures': 'str',
    'type_of_set_aside': 'str',
    'evaluated_preference': 'str',
    'number_of_offers_received': 'Int64',
    'commercial_item_acquisition_procedures': 'str',
    'fed_biz_opps': 'str',
    'price_evaluation_adjustment_preference_percent_difference': 'float',
    'interagency_contracting_authority': 'str',
    'national_interest_action': 'str',
    'cost_or_pricing_data': 'str',
    'performance_based_service_acquisition': 'str',
    'multi_year_contract': 'str',
    'contract_financing': 'str',
    'contracting_officers_determination_of_business_size': 'str',
    'organizational_type': 'str',
    'dot_certified_disadvantage': 'str',
    'self_certified_small_disadvantaged_business': 'str',
    'small_disadvantaged_business': 'str',
    'c8a_program_participant': 'str',
    'historically_underutilized_business_zone_hubzone_firm': 'str',
    'sba_certified_8a_joint_venture': 'str',
    'usaspending_permalink': 'str',
    'initial_report_date': 'str',
    'last_modified_date': 'str',
}

# Specify the fields to save in the output file
fields_to_save = [
'contract_award_unique_key',
'award_id_piid',
'modification_number',
'transaction_number',
'federal_action_obligation',
'total_dollars_obligated',
'base_and_exercised_options_value',
'current_total_value_of_award',
'base_and_all_options_value',
'potential_total_value_of_award',
'action_date',
'action_date_fiscal_year',
'period_of_performance_start_date',
'period_of_performance_current_end_date',
'period_of_performance_potential_end_date',
'ordering_period_end_date',
'solicitation_date',
'awarding_agency_name',
'awarding_sub_agency_name',
'awarding_office_name',
'funding_agency_name',
'funding_sub_agency_name',
'funding_office_name',
'recipient_uei',
'recipient_name',
'recipient_name_raw',
'cage_code',
'recipient_parent_uei',
'recipient_parent_name',
'recipient_parent_name_raw',
'recipient_country_code',
'recipient_address_line_1',
'recipient_city_name',
'recipient_county_name',
'recipient_state_code',
'recipient_state_name',
'recipient_zip_4_code',
'recipient_phone_number',
'primary_place_of_performance_country_code',
'primary_place_of_performance_city_name',
'primary_place_of_performance_state_code',
'award_or_idv_flag',
'award_type',
'type_of_contract_pricing',
'transaction_description',
'prime_award_base_transaction_description',
'action_type',
'solicitation_identifier',
'number_of_actions',
'inherently_governmental_functions_description',
'product_or_service_code',
'product_or_service_code_description',
'contract_bundling',
'dod_claimant_program_description',
'naics_code',
'naics_description',
'information_technology_commercial_item_category',
'subcontracting_plan',
'extent_competed',
'solicitation_procedures',
'type_of_set_aside',
'evaluated_preference',
'number_of_offers_received',
'commercial_item_acquisition_procedures',
'fed_biz_opps',
'price_evaluation_adjustment_preference_percent_difference',
'interagency_contracting_authority',
'national_interest_action',
'cost_or_pricing_data',
'performance_based_service_acquisition',
'multi_year_contract',
'contract_financing',
'contracting_officers_determination_of_business_size',
'organizational_type',
'dot_certified_disadvantage',
'self_certified_small_disadvantaged_business',
'small_disadvantaged_business',
'c8a_program_participant',
'historically_underutilized_business_zone_hubzone_firm',
'sba_certified_8a_joint_venture',
'usaspending_permalink',
'initial_report_date',
'last_modified_date',
]

# Read the CSV file in chunks and skip bad lines
def filter_data(input_file, output_file, field_name, filter_hash_set, chunk_size):

    # Initialize a counter for skipped lines and total processed records
    skipped_lines_count = 0
    total_processed_count = 0  # Counter for total records processed

    # Initialize an array to hold filtered records from all files
    all_filtered_data = []

    # use chunksize to lower memory needs, typically in multiples of 100,000
    for chunk in pd.read_csv(input_file, dtype=dtype_mapping, encoding='utf-16', chunksize=chunk_size):

        # Update the total processed count
        total_processed_count += len(chunk)

        # Filter the DataFrame based on the psc codes 
        filtered_chunk = chunk[chunk[field_name].isin(filter_hash_set)]  # Include limited psc codes of interest

        # Append the filtered records to the list
        all_filtered_data.append(filtered_chunk)

        # Print the count of records processed
        # print(f"Number of skipped lines in {input_file}: {skipped_lines_count}")
        print(f"Processed {total_processed_count} records from {input_file}.")

    if all_filtered_data:
        # Concatenate all filtered data
        final_filtered_df = pd.concat(all_filtered_data, ignore_index=True)
        
    """Saves the filtered DataFrame to a CSV file with the specified fields."""
    if not filtered_chunk.empty:
        # Select only the desired fields
        final_filtered_df = filtered_chunk[fields_to_save]

        # Save the filtered DataFrame to a single output file with one header
        final_filtered_df.to_csv(output_file, index=False)

        # Output the number of records saved
        print(f"Filtered records saved to: {output_file}")
        print(f"Number of records saved: {len(final_filtered_df)}")
    else:
        print("No records found matching the specified product_or_service_code values.")


# Start timing the processing
start_time = time.time()

# Define the input directory and output directory
input_file = r"C:\temp\awards\out\dod_awards_by_naics_codes.csv" 
output_file = r"C:\temp\awards\out\dod_awards_by_naics_and_psc_codes_isnotin.csv"

chunk_size = 50000
filter_data(input_file, output_file, 'product_or_service_code', psc_codes_hash_set, chunk_size)

#End the timer to measure total script elapsed time
script_duration = time.time() - script_start_time

# Convert duration into hours, minutes, and seconds for readability
hours, remainder = divmod(script_duration, 3600)
minutes, seconds = divmod(remainder, 60)

# Print user-friendly execution time
print(f"Script processing time: {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")