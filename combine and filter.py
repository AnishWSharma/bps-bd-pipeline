import pandas as pd
import os
import time
import chardet

def detect_file_encoding(file_path):
    """Detects the encoding of a given file and returns it for use in pd.read_csv."""
    try:
        with open(file_path, 'rb') as file:
            raw_data = file.read(10000)  # Read the first 10000 bytes
            result = chardet.detect(raw_data)
            encoding = result['encoding']
            encoding = 'utf-8' if encoding == 'ascii' else encoding
            confidence = result['confidence']
            print(f"Detected encoding for {file_path}: {encoding} with confidence {confidence*100:.2f}")
            return encoding  # Return the detected encoding
    except Exception as e:
        print(f"An error occurred: {e}")
        return None  # Return None in case of an error

# A function to concatenate all filtered records into a single DataFrame by type of funding_agency (federal civilian or dod)
def save_filtered_data(filtered_chunk, fields_to_save, output_file_path):
    """Saves a DataFrame to a CSV file with the specified fields."""
    if not filtered_chunk.empty:
        # Select only the desired fields
        final_filtered_df = filtered_chunk[fields_to_save]

        # Save the filtered DataFrame to a single output file with one header
        final_filtered_df.to_csv(output_file_path, index=False, encoding='utf-8') # save in utf-8 for maximum comptability

        # Output the number of records saved
        print(f"Filtered records saved to: {output_file_path}")
        print(f"Number of records saved: {len(final_filtered_df)}")
    else:
        print("No records found matching the specified product_or_service_code values across all files.")

# A function to process all csv files and filter based on NACICS, PSC codes, and type of agency (either fedciv or dod)
def combine_and_filter_data(input_directory,output_directory,naics_codes_hash_set,psc_codes_hash_set):

    output_file_name_fedciv = "combined_fedciv.csv"
    output_file_name_dod = "combined_dod.csv"

    output_file_path_fedciv = os.path.join(output_directory, output_file_name_fedciv)
    output_file_path_dod = os.path.join(output_directory, output_file_name_dod)

    # Process each CSV file in the input directory
    for filename in os.listdir(input_directory):
        if filename.endswith('.csv'):
            file_processing_start_time = time.time()
            input_file_path = os.path.join(input_directory, filename)

            # Initialize a counter for skipped lines and total processed records
            skipped_lines_count = 0
            total_processed_count = 0  # Counter for total records processed

            # Read the CSV file in chunks and skip bad lines
            # use small chunksize to lower memory needs
            chunk_size = 250000
            file_encoding = detect_file_encoding(input_file_path)
            chunk_processing_start_time = time.time()
            for chunk in pd.read_csv(input_file_path, dtype=dtype_mapping, chunksize=chunk_size, encoding=file_encoding, on_bad_lines='skip'):
                
                # Count skipped lines in the current chunk
                skipped_lines_count += chunk.shape[0] - len(chunk.dropna())

                # Update the total processed count
                total_processed_count += len(chunk)

                # Filter the DataFrame based on the NAICS codes and PSC codes.  Also weed out records which are actions related to GSA MAS schedule
                filtered_chunk = chunk[
                    (chunk['naics_code'].isin(naics_codes_hash_set)) & 
                    (chunk['product_or_service_code'].isin(psc_codes_hash_set)) &
                    (chunk['awarding_sub_agency_name'] != 'Federal Acquisition Service')       # Indicator of MAS Schedule actions which we don't care about
                ]

                # Further filter based on the funding agency name
                filtered_chunk_fedciv = filtered_chunk[filtered_chunk['funding_agency_name'] != 'Department of Defense']  # Exclude DoD records
                filtered_chunk_dod = filtered_chunk[filtered_chunk['funding_agency_name'] == 'Department of Defense']  # Include DoD records

                # Append the filtered records to the list
                all_filtered_fedciv_data.append(filtered_chunk_fedciv)
                all_filtered_dod_data.append(filtered_chunk_dod)

                chunk_processing_duration = time.time() - chunk_processing_start_time
                
                # Convert duration into hours, minutes, and seconds for readability
                chunk_hours, chunk_remainder = divmod(chunk_processing_duration, 3600)
                chunk_minutes, chunk_seconds = divmod(chunk_remainder, 60)

                # Print user-friendly execution time for each chunk
                print(f"\t{total_processed_count} records \t\t: {int(chunk_hours)} hours, {int(chunk_minutes)} minutes, {int(chunk_seconds)} seconds")

    if all_filtered_fedciv_data:
        # Concatenate all filtered FedCiv data
        final_filtered_fedciv_df = pd.concat(all_filtered_fedciv_data, ignore_index=True)
        
        # Call the function to save filtered FedCiv data
        save_filtered_data(final_filtered_fedciv_df, fields_to_save, output_file_path_fedciv)

    if all_filtered_dod_data:
        # Concatenate all filtered DoD data
        final_filtered_dod_df = pd.concat(all_filtered_dod_data, ignore_index=True)
        
        # Call the function to save filtered DoD data
        save_filtered_data(final_filtered_dod_df, fields_to_save, output_file_path_dod)


#start a timer to measure total elapsed time
script_start_time = time.time()

# Define the list of Product or Service Codes to filter
# 10/9/2024 AS: Do not filter by these codes as there are several codes with prefix 'D' which pertain to IT.  BIA ITSS award is DF01
# So for now, only use NAICS codes to weed out unneeded records.
# psc_codes_to_filter = ["R499", "D399", "D306", "R408", "R410", "D308", "D318", "D301", "DC01", "DA01", "DF01"]
# Convert the codes to a set for faster lookup
# psc_codes_hash_set = set(psc_codes_to_filter)

# Define data types for columns based on the original list of field types
dtype_mapping = {
    'contract_transaction_unique_key': 'str',
    'contract_award_unique_key': 'str',
    'award_id_piid': 'str',
    'modification_number': 'str',
    'transaction_number': 'str',
    'parent_award_agency_id': 'str',
    'parent_award_agency_name': 'str',
    'parent_award_id_piid': 'str',
    'parent_award_modification_number': 'str',
    'federal_action_obligation': 'float',
    'total_dollars_obligated': 'float',
    'total_outlayed_amount_for_overall_award': 'float',
    'base_and_exercised_options_value': 'float',
    'current_total_value_of_award': 'float',
    'base_and_all_options_value': 'float',
    'potential_total_value_of_award': 'float',
    'disaster_emergency_fund_codes_for_overall_award': 'str',
    'outlayed_amount_from_COVID-19_supplementals_for_overall_award': 'float',
    'obligated_amount_from_COVID-19_supplementals_for_overall_award': 'float',
    'outlayed_amount_from_IIJA_supplemental_for_overall_award': 'float',
    'obligated_amount_from_IIJA_supplementals_for_overall_award': 'float',
    'action_date': 'str',  # Using str to handle various date formats
    'action_date_fiscal_year': 'Int64',
    'period_of_performance_start_date': 'str',
    'period_of_performance_current_end_date': 'str',
    'period_of_performance_potential_end_date': 'str',
    'ordering_period_end_date': 'str',
    'solicitation_date': 'str',
    'awarding_agency_code': 'str',
    'awarding_agency_name': 'str',
    'awarding_sub_agency_code': 'str',
    'awarding_sub_agency_name': 'str',
    'awarding_office_code': 'str',
    'awarding_office_name': 'str',
    'funding_agency_code': 'str',
    'funding_agency_name': 'str',
    'funding_sub_agency_code': 'str',
    'funding_sub_agency_name': 'str',
    'funding_office_code': 'str',
    'funding_office_name': 'str',
    'treasury_accounts_funding_this_award': 'str',
    'federal_accounts_funding_this_award': 'str',
    'object_classes_funding_this_award': 'str',
    'program_activities_funding_this_award': 'str',
    'foreign_funding': 'str',
    'foreign_funding_description': 'str',
    'sam_exception': 'str',
    'sam_exception_description': 'str',
    'recipient_uei': 'str',
    'recipient_duns': 'str',
    'recipient_name': 'str',
    'recipient_name_raw': 'str',
    'recipient_doing_business_as_name': 'str',
    'cage_code': 'str',
    'recipient_parent_uei': 'str',
    'recipient_parent_duns': 'str',
    'recipient_parent_name': 'str',
    'recipient_parent_name_raw': 'str',
    'recipient_country_code': 'str',
    'recipient_country_name': 'str',
    'recipient_address_line_1': 'str',
    'recipient_address_line_2': 'str',
    'recipient_city_name': 'str',
    'prime_award_transaction_recipient_county_fips_code': 'str',
    'recipient_county_name': 'str',
    'prime_award_transaction_recipient_state_fips_code': 'str',
    'recipient_state_code': 'str',
    'recipient_state_name': 'str',
    'recipient_zip_4_code': 'str',
    'prime_award_transaction_recipient_cd_original': 'str',
    'prime_award_transaction_recipient_cd_current': 'str',
    'recipient_phone_number': 'str',
    'recipient_fax_number': 'str',
    'primary_place_of_performance_country_code': 'str',
    'primary_place_of_performance_country_name': 'str',
    'primary_place_of_performance_city_name': 'str',
    'prime_award_transaction_place_of_performance_county_fips_code': 'str',
    'primary_place_of_performance_county_name': 'str',
    'prime_award_transaction_place_of_performance_state_fips_code': 'str',
    'primary_place_of_performance_state_code': 'str',
    'primary_place_of_performance_state_name': 'str',
    'primary_place_of_performance_zip_4': 'str',
    'prime_award_transaction_place_of_performance_cd_original': 'str',
    'prime_award_transaction_place_of_performance_cd_current': 'str',
    'award_or_idv_flag': 'str',
    'award_type_code': 'str',
    'award_type': 'str',
    'idv_type_code': 'str',
    'idv_type': 'str',
    'multiple_or_single_award_idv_code': 'str',
    'multiple_or_single_award_idv': 'str',
    'type_of_idc_code': 'str',
    'type_of_idc': 'str',
    'type_of_contract_pricing_code': 'str',
    'type_of_contract_pricing': 'str',
    'transaction_description': 'str',
    'prime_award_base_transaction_description': 'str',
    'action_type_code': 'str',
    'action_type': 'str',
    'solicitation_identifier': 'str',
    'number_of_actions': 'Int64',
    'inherently_governmental_functions': 'str',
    'inherently_governmental_functions_description': 'str',
    'product_or_service_code': 'str',
    'product_or_service_code_description': 'str',
    'contract_bundling_code': 'str',
    'contract_bundling': 'str',
    'dod_claimant_program_code': 'str',
    'dod_claimant_program_description': 'str',
    'naics_code': 'str',
    'naics_description': 'str',
    'recovered_materials_sustainability_code': 'str',
    'recovered_materials_sustainability': 'str',
    'domestic_or_foreign_entity_code': 'str',
    'domestic_or_foreign_entity': 'str',
    'dod_acquisition_program_code': 'str',
    'dod_acquisition_program_description': 'str',
    'information_technology_commercial_item_category_code': 'str',
    'information_technology_commercial_item_category': 'str',
    'epa_designated_product_code': 'str',
    'epa_designated_product': 'str',
    'country_of_product_or_service_origin_code': 'str',
    'country_of_product_or_service_origin': 'str',
    'place_of_manufacture_code': 'str',
    'place_of_manufacture': 'str',
    'subcontracting_plan_code': 'str',
    'subcontracting_plan': 'str',
    'extent_competed_code': 'str',
    'extent_competed': 'str',
    'solicitation_procedures_code': 'str',
    'solicitation_procedures': 'str',
    'type_of_set_aside_code': 'str',
    'type_of_set_aside': 'str',
    'evaluated_preference_code': 'str',
    'evaluated_preference': 'str',
    'research_code': 'str',
    'research': 'str',
    'fair_opportunity_limited_sources_code': 'str',
    'fair_opportunity_limited_sources': 'str',
    'other_than_full_and_open_competition_code': 'str',
    'other_than_full_and_open_competition': 'str',
    'number_of_offers_received': 'Int64',
    'commercial_item_acquisition_procedures_code': 'str',
    'commercial_item_acquisition_procedures': 'str',
    'small_business_competitiveness_demonstration_program': 'str',
    'simplified_procedures_for_certain_commercial_items_code': 'str',
    'simplified_procedures_for_certain_commercial_items': 'str',
    'a76_fair_act_action_code': 'str',
    'a76_fair_act_action': 'str',
    'fed_biz_opps_code': 'str',
    'fed_biz_opps': 'str',
    'local_area_set_aside_code': 'str',
    'local_area_set_aside': 'str',
    'price_evaluation_adjustment_preference_percent_difference': 'float',
    'clinger_cohen_act_planning_code': 'str',
    'clinger_cohen_act_planning': 'str',
    'materials_supplies_articles_equipment_code': 'str',
    'materials_supplies_articles_equipment': 'str',
    'labor_standards_code': 'str',
    'labor_standards': 'str',
    'construction_wage_rate_requirements_code': 'str',
    'construction_wage_rate_requirements': 'str',
    'interagency_contracting_authority_code': 'str',
    'interagency_contracting_authority': 'str',
    'other_statutory_authority': 'str',
    'program_acronym': 'str',
    'parent_award_type_code': 'str',
    'parent_award_type': 'str',
    'parent_award_single_or_multiple_code': 'str',
    'parent_award_single_or_multiple': 'str',
    'major_program': 'str',
    'national_interest_action_code': 'str',
    'national_interest_action': 'str',
    'cost_or_pricing_data_code': 'str',
    'cost_or_pricing_data': 'str',
    'cost_accounting_standards_clause_code': 'str',
    'cost_accounting_standards_clause': 'str',
    'government_furnished_property_code': 'str',
    'government_furnished_property': 'str',
    'sea_transportation_code': 'str',
    'sea_transportation': 'str',
    'undefinitized_action_code': 'str',
    'undefinitized_action': 'str',
    'consolidated_contract_code': 'str',
    'consolidated_contract': 'str',
    'performance_based_service_acquisition_code': 'str',
    'performance_based_service_acquisition': 'str',
    'multi_year_contract_code': 'str',
    'multi_year_contract': 'str',
    'contract_financing_code': 'str',
    'contract_financing': 'str',
    'purchase_card_as_payment_method_code': 'str',
    'purchase_card_as_payment_method': 'str',
    'contingency_humanitarian_or_peacekeeping_operation_code': 'str',
    'contingency_humanitarian_or_peacekeeping_operation': 'str',
    'alaskan_native_corporation_owned_firm': 'str',
    'american_indian_owned_business': 'str',
    'indian_tribe_federally_recognized': 'str',
    'native_hawaiian_organization_owned_firm': 'str',
    'tribally_owned_firm': 'str',
    'veteran_owned_business': 'str',
    'service_disabled_veteran_owned_business': 'str',
    'woman_owned_business': 'str',
    'women_owned_small_business': 'str',
    'economically_disadvantaged_women_owned_small_business': 'str',
    'joint_venture_women_owned_small_business': 'str',
    'joint_venture_economic_disadvantaged_women_owned_small_bus': 'str',
    'minority_owned_business': 'str',
    'subcontinent_asian_asian_indian_american_owned_business': 'str',
    'asian_pacific_american_owned_business': 'str',
    'black_american_owned_business': 'str',
    'hispanic_american_owned_business': 'str',
    'native_american_owned_business': 'str',
    'other_minority_owned_business': 'str',
    'contracting_officers_determination_of_business_size': 'str',
    'contracting_officers_determination_of_business_size_code': 'str',
    'emerging_small_business': 'str',
    'community_developed_corporation_owned_firm': 'str',
    'labor_surplus_area_firm': 'str',
    'us_federal_government': 'str',
    'federally_funded_research_and_development_corp': 'str',
    'federal_agency': 'str',
    'us_state_government': 'str',
    'us_local_government': 'str',
    'city_local_government': 'str',
    'county_local_government': 'str',
    'inter_municipal_local_government': 'str',
    'local_government_owned': 'str',
    'municipality_local_government': 'str',
    'school_district_local_government': 'str',
    'township_local_government': 'str',
    'us_tribal_government': 'str',
    'foreign_government': 'str',
    'organizational_type': 'str',
    'corporate_entity_not_tax_exempt': 'str',
    'corporate_entity_tax_exempt': 'str',
    'partnership_or_limited_liability_partnership': 'str',
    'sole_proprietorship': 'str',
    'small_agricultural_cooperative': 'str',
    'international_organization': 'str',
    'us_government_entity': 'str',
    'community_development_corporation': 'str',
    'domestic_shelter': 'str',
    'educational_institution': 'str',
    'foundation': 'str',
    'hospital_flag': 'str',
    'manufacturer_of_goods': 'str',
    'veterinary_hospital': 'str',
    'hispanic_servicing_institution': 'str',
    'receives_contracts': 'str',
    'receives_financial_assistance': 'str',
    'receives_contracts_and_financial_assistance': 'str',
    'airport_authority': 'str',
    'council_of_governments': 'str',
    'housing_authorities_public_tribal': 'str',
    'interstate_entity': 'str',
    'planning_commission': 'str',
    'port_authority': 'str',
    'transit_authority': 'str',
    'subchapter_scorporation': 'str',
    'limited_liability_corporation': 'str',
    'foreign_owned': 'str',
    'for_profit_organization': 'str',
    'nonprofit_organization': 'str',
    'other_not_for_profit_organization': 'str',
    'the_ability_one_program': 'str',
    'private_university_or_college': 'str',
    'state_controlled_institution_of_higher_learning': 'str',
    '1862_land_grant_college': 'str',
    '1890_land_grant_college': 'str',
    '1994_land_grant_college': 'str',
    'minority_institution': 'str',
    'historically_black_college': 'str',
    'tribal_college': 'str',
    'alaskan_native_servicing_institution': 'str',
    'native_hawaiian_servicing_institution': 'str',
    'school_of_forestry': 'str',
    'veterinary_college': 'str',
    'dot_certified_disadvantage': 'str',
    'self_certified_small_disadvantaged_business': 'str',
    'small_disadvantaged_business': 'str',
    'c8a_program_participant': 'str',
    'historically_underutilized_business_zone_hubzone_firm': 'str',
    'sba_certified_8a_joint_venture': 'str',
    'highly_compensated_officer_1_name': 'str',
    'highly_compensated_officer_1_amount': 'float',
    'highly_compensated_officer_2_name': 'str',
    'highly_compensated_officer_2_amount': 'float',
    'highly_compensated_officer_3_name': 'str',
    'highly_compensated_officer_3_amount': 'float',
    'highly_compensated_officer_4_name': 'str',
    'highly_compensated_officer_4_amount': 'float',
    'highly_compensated_officer_5_name': 'str',
    'highly_compensated_officer_5_amount': 'float',
    'usaspending_permalink': 'str',
    'initial_report_date': 'str',
    'last_modified_date': 'str',
}

# Specify the fields to save in the output file
fields_to_save = [
'contract_transaction_unique_key',
'potential_total_value_of_award',
'recipient_name',
'transaction_description',
'contract_award_unique_key',
'award_id_piid',
'modification_number',
'transaction_number',
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
'cage_code',
'recipient_parent_uei',
'recipient_parent_name',
'primary_place_of_performance_city_name',
'primary_place_of_performance_state_code',
'award_or_idv_flag',
'award_type',
'idv_type',
'multiple_or_single_award_idv',
'type_of_contract_pricing',
'action_type',
'solicitation_identifier',
'product_or_service_code',
'product_or_service_code_description',
'naics_code',
'extent_competed',
'type_of_set_aside',
'number_of_offers_received',
'contracting_officers_determination_of_business_size',
'sba_certified_8a_joint_venture',
'usaspending_permalink',
]

# Define the list of NAICS codes to include in the final dataset
naics_codes_to_filter = [
    "541511", "541512", "541513", "541519", 
    "541611", "541612", "541613", "541614", 
    "541618", "541620", "541690"
]

# Convert the NAICS codes to a set for faster lookup
naics_codes_hash_set = set(naics_codes_to_filter)

# Define the list of PSC codes to include in the final dataset.
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

# Define the input directory and output directory
input_directory = r"C:\temp\awards"  
output_directory = os.path.join(input_directory, "out")

# Create the output directory if it does not exist
os.makedirs(output_directory, exist_ok=True)

# Initialize an array to hold filtered records from all files
all_filtered_fedciv_data = []
all_filtered_dod_data = []

combine_and_filter_data(input_directory,output_directory,naics_codes_hash_set,psc_codes_hash_set)
#End the timer to measure total script elapsed time
script_duration = time.time() - script_start_time

# Convert duration into hours, minutes, and seconds for readability
hours, remainder = divmod(script_duration, 3600)
minutes, seconds = divmod(remainder, 60)

# Print user-friendly execution time
print(f"Script processing time: {int(hours)} hours, {int(minutes)} minutes, {int(seconds)} seconds")