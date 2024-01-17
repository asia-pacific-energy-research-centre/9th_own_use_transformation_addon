# Finalising accounting of supply components that are not coal, oil and gas
# Set working directory to be the project folder 
import os
import re

wanted_wd = '9th_supply_components'
os.chdir(re.split(wanted_wd, os.getcwd())[0] + wanted_wd)

# execute config file
config_file = './config/config_oct2023.py'
with open(config_file) as infile:
    exec(infile.read())

# Grab APEC economies (economy_list defined in config file)
APEC_economies = list(economy_list)[:-7]
APEC_economies = APEC_economies[18:19]

# 2021 and beyond
proj_years = list(range(2022, 2071, 1))
proj_years_str = [str(i) for i in proj_years]

latest_hist = '2021'

# latest EGEDA data
EGEDA_df = pd.read_csv(latest_EGEDA)
EGEDA_df = EGEDA_df.drop(columns = ['is_subtotal']).copy().reset_index(drop = True)

# sub1sectors transformation categories that need to be modelled
biomass_subfuel_df = pd.read_csv('./data/config/biomass_subfuels.csv', header = None)
others_subfuel_df = pd.read_csv('./data/config/others_subfuels.csv', header = None)

subfuels_list = biomass_subfuel_df[0].values.tolist() + others_subfuel_df[0].values.tolist()

relevant_supply = ['01_production', '02_imports', '03_exports']
all_supply = ['01_production', '02_imports', '03_exports', '04_international_marine_bunkers', '05_international_aviation_bunkers',
              '06_stock_changes', '07_total_primary_energy_supply']

for economy in APEC_economies:
    # Save location
    save_location = './results/02_supply_results/{}/'.format(economy)

    if not os.path.isdir(save_location):
        os.makedirs(save_location)

    # This is the location where the merged TFC and transformation results are provided
    modelled_result = './data/copy 06b_ready_for_TPES here/'

    # Read in results dataframe
    file_prefix = 'merged_file_' + economy

    # Define vector with file names
    files = glob.glob(modelled_result + file_prefix + '*.csv')

    # Identify the most uptodate file
    if len(files) > 0:
        latest_file = max(files, key = os.path.getctime)
        merged_file_date = re.search(r'(\d{4})(\d{2})(\d{2})', latest_file).group(0)

        results_df = pd.read_csv(latest_file)
        results_ref = results_df[results_df['scenarios'] == 'reference'].copy().reset_index(drop = True)
        results_tgt = results_df[results_df['scenarios'] == 'target'].copy().reset_index(drop = True)

        scenario_dict = {'ref': [results_ref],
                         'tgt': [results_tgt]}
        
        for scenario in scenario_dict.keys():
            scenario_results_df = scenario_dict[scenario][0]
            # Start with biomass and 16_others fuels that are not modelled by biorefining model
            # Creat empty dataframe to save results
            supply_df = pd.DataFrame()
            
            for fuel in subfuels_list:
                # Consumption results are: TFC, transformation and own-use
                # Transformation
                trans_ref = scenario_results_df[(scenario_results_df['subfuels'] == fuel) &
                                        (scenario_results_df['sectors'] == '09_total_transformation_sector') &
                                        (scenario_results_df['sub1sectors'] == 'x')].copy().reset_index(drop = True).fillna(0).iloc[:,:-3]
                
                # Transformation is negative so need to be made positive to calculate total consumption
                numeric_trans = trans_ref.iloc[:, 9:] * -1
                trans_ref.iloc[:, 9:] = numeric_trans
                
                # Own-use
                own_ref = scenario_results_df[(scenario_results_df['subfuels'] == fuel) &
                                    (scenario_results_df['sectors'] == '10_losses_and_own_use') &
                                    (scenario_results_df['sub1sectors'] == 'x')].copy().reset_index(drop = True).fillna(0).iloc[:,:-3]
                
                # Own-use is negative so need to be made positive to calculate total consumption
                numeric_own = own_ref.iloc[:, 9:] * -1
                own_ref.iloc[:, 9:] = numeric_own
                
                # TFC
                tfc_ref = scenario_results_df[(scenario_results_df['subfuels'] == fuel) &
                                    (scenario_results_df['sectors'] == '12_total_final_consumption') &
                                    (scenario_results_df['sub1sectors'] == 'x')].copy().reset_index(drop = True).fillna(0).iloc[:,:-3]
                
                # Combine
                all_cons = pd.concat([trans_ref, own_ref, tfc_ref]).copy().reset_index(drop = True)

                # Generate total row
                total_row = all_cons.groupby(['scenarios', 'economy', 'sub1sectors', 'sub2sectors', 
                                                'sub3sectors', 'sub4sectors', 'fuels', 'subfuels'])\
                                                    .sum().assign(sectors = 'Total consumption').reset_index()

                # Add the row to the other consumption rows
                all_cons = pd.concat([all_cons, total_row]).copy().reset_index(drop = True)

                # Now grab TPES, but just for 2021 in order to get a ratio and apply it for projected results
                current_supply = scenario_results_df[(scenario_results_df['subfuels'] == fuel) &
                                            (scenario_results_df['sectors'].isin(relevant_supply)) &
                                            (scenario_results_df['sub1sectors'] == 'x')].copy().reset_index(drop = True)\
                                                .fillna(0).loc[:, ['sectors', latest_hist]]
                
                # Create new column for ratio results
                current_supply['ratio'] = np.nan

                # Calculate ratio
                for row in current_supply.index:
                    if current_supply[latest_hist].sum() == 0:
                        current_supply.loc[row, 'ratio'] = 0

                    else:
                        current_supply.loc[row, 'ratio'] = current_supply.loc[row, latest_hist] / current_supply[latest_hist].sum()

                # Supply results df to fill in
                subfuels_supply_df = scenario_results_df[(scenario_results_df['subfuels'] == fuel) &
                                            (scenario_results_df['sectors'].isin(all_supply)) &
                                            (scenario_results_df['sub1sectors'] == 'x')].copy().reset_index(drop = True).iloc[:, :-3]
                
                # Calculate production, imports and exports for each projection year for every subfuel defined in the subfuels list
                for year in proj_years_str:
                    for component in relevant_supply:
                        subfuels_supply_df.loc[subfuels_supply_df['sectors'] == component, year] = all_cons.loc[all_cons['sectors'] == 'Total consumption', year].values[0]\
                            * current_supply.loc[current_supply['sectors'] == component, 'ratio'].values[0]

                supply_df = pd.concat([supply_df, subfuels_supply_df]).copy().reset_index(drop = True)

            supply_df.to_csv(save_location + economy + '_biomass_others_supply_' + scenario + '_' + timestamp + '.csv', index = False)
                    
            







# What do we need to do here
# 1. Grab fuel consumption for each of these non-major (sub)fuels for all projection years
# 2. Grab split of supply components: production, imports, exports, bunkers and stock changes to arrive at TPES
# --> production, imports, exports have not been calculated yet
# --> Assume stock changes are zero
# --> Grab bunkers results that have already been generated to 2070
# --> From the above: Production + imports - exports - bunker result = TPES
# For most recent historical year (2020 or 2021), look at the ratio of 
# --> Production to TPES
# --> Imports to TPES
# --> Exports to TPES 

# Coal products
# No production