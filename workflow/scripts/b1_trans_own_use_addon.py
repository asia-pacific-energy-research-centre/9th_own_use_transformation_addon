# Modelling 9_transformation and 10_own_use_and_losses that have not been modelled by other sectors
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

# 2022 and beyond
proj_years = list(range(2022, 2071, 1))
proj_years_str = [str(i) for i in proj_years]

# latest EGEDA data
EGEDA_df = pd.read_csv(latest_EGEDA)
EGEDA_df = EGEDA_df.drop(columns = ['is_subtotal']).copy().reset_index(drop = True)

EGEDA_df[EGEDA_df['sub1sectors'].str.startswith('09')]['sub1sectors'].unique()
EGEDA_df[EGEDA_df['sub1sectors'].str.startswith('10')]['sub2sectors'].unique()

# sub1sectors transformation categories that need to be modelled
trans_df = pd.read_csv('./data/config/transformation_orphans.csv', header = None)
trans_cats = trans_df[0].values.tolist()

# sub2sectors own-use categories that need to be modelled
ownuse_df = pd.read_csv('./data/config/ownuse_cats.csv', header = None)
ownuse_cats = ownuse_df[0].values.tolist()

relevant_fuels = ['01_coal', '02_coal_products', '06_crude_oil_and_ngl', '07_petroleum_products',
                  '08_gas', '15_solid_biomass', '16_others', '17_electricity', '18_heat']

EGEDA_trans = EGEDA_df[EGEDA_df['sub1sectors'].isin(trans_cats)].copy().reset_index(drop = True)
EGEDA_own = EGEDA_df[EGEDA_df['sub2sectors'].isin(ownuse_cats)].copy().reset_index(drop = True)

for economy in APEC_economies:
    # Save location
    save_location = './results/02_trans_own_addon/{}/'.format(economy)

    if not os.path.isdir(save_location):
        os.makedirs(save_location)

    # This is the location where the merged TFC and transformation results are provided
    modelled_result = './data/copy 02_TFC here/'

    file_prefix = 'merged_file*' + economy

    # Define vector with file names
    files = glob.glob(modelled_result + file_prefix + '*.csv')

    # Identify the most uptodate file
    if len(files) > 0:
        latest_file = max(files, key = os.path.getctime)
        merged_file_date = re.search(r'(\d{4})(\d{2})(\d{2})', latest_file).group(0)

        results_df = pd.read_csv(latest_file)
        results_ref = results_df[results_df['scenarios'] == 'reference'].copy().reset_index(drop = True)
        results_tgt = results_df[results_df['scenarios'] == 'target'].copy().reset_index(drop = True)

        # Transformation results needed
        EGEDA_trans_ref = EGEDA_trans[EGEDA_trans['economy'] == economy].copy().reset_index(drop = True)
        EGEDA_trans_tgt = EGEDA_trans[EGEDA_trans['economy'] == economy].copy().reset_index(drop = True)
        EGEDA_trans_tgt['scenarios'] = 'target'

        # Own-use results needed
        EGEDA_own_ref = EGEDA_own[EGEDA_own['economy'] == economy].copy().reset_index(drop = True)
        EGEDA_own_tgt = EGEDA_own[EGEDA_own['economy'] == economy].copy().reset_index(drop = True)
        EGEDA_own_tgt['scenarios'] = 'target'

        scenario_dict = {'ref': [results_ref, EGEDA_trans_ref, EGEDA_own_ref],
                         'tgt': [results_tgt, EGEDA_trans_tgt, EGEDA_own_tgt]}
        
        for scenario in scenario_dict.keys():
            # Data frame with results from other sectors to use to build trajectories to fill the trans and own df's
            tfc_df = scenario_dict[scenario][0]
            # Subset so only consumption categories are included
            # Transformation results below no longer used. Only TFC trajectories are used for modelling of categories defined in this script
            # trans_df = tfc_trans_df[(tfc_trans_df['sectors'].isin(['09_total_transformation_sector'])) &
            #                         (tfc_trans_df['sub1sectors'] == 'x')].copy().reset_index(drop = True)
            
            # for column in trans_df.columns:
            #     if pd.api.types.is_numeric_dtype(trans_df[column]):
            #         trans_df[column] = trans_df[column].abs()

            tfc_df = tfc_df[(tfc_df['sectors'].isin(['12_total_final_consumption'])) &
                            (tfc_df['sub1sectors'] == 'x')].copy().reset_index(drop = True)

            # Subset so its only high level fuels
            tfc_df = tfc_df[tfc_df['subfuels'] == 'x']
                # .groupby(['scenarios', 'economy', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'subfuels', 'fuels'])\
                #     .sum().reset_index().assign(sectors = 'TFC consumption')
            
            # Now only keep relevant fuels 
            tfc_df = tfc_df[tfc_df['fuels'].isin(relevant_fuels)].copy().reset_index(drop = True)
            tfc_df = tfc_df.fillna(0)
            
            # Dataframes to populate
            trans_df = scenario_dict[scenario][1]
            trans_df = trans_df.fillna(0)
            own_df = scenario_dict[scenario][2]
            own_df = own_df.fillna(0)

            # Define results dataframe
            trans_results_df = pd.DataFrame(columns = trans_df.columns)
            own_results_df = pd.DataFrame(columns = own_df.columns)

            for fuel in tfc_df['fuels'].unique():
                fuel_agg_row = tfc_df[tfc_df['fuels'] == fuel].copy().reset_index(drop = True)

                trans_results_interim = trans_df[trans_df['fuels'] == fuel].copy().reset_index(drop = True)
                own_results_interim = own_df[own_df['fuels'] == fuel].copy().reset_index(drop = True)

                for year in proj_years_str:
                    if fuel_agg_row.loc[0, str(int(year) - 1)] == 0:
                        ratio = 0

                    else:
                        ratio = fuel_agg_row.loc[0, year] / fuel_agg_row.loc[0, str(int(year) - 1)]
                    
                    # Now populate the transformation and own use dataframe based on the growth ratio calculated above
                    for row in trans_results_interim.index:
                        trans_results_interim.loc[row, year] = trans_results_interim.loc[row, str(int(year) - 1)] * ratio

                    for row in own_results_interim.index:
                        own_results_interim.loc[row, year] = own_results_interim.loc[row, str(int(year) - 1)] * ratio

                if trans_results_df.empty:
                    trans_results_df = trans_results_interim.copy()
                else:             
                    trans_results_df = pd.concat([trans_results_df, trans_results_interim]).copy().reset_index(drop = True)
                
                if own_results_df.empty:
                    own_results_df = own_results_interim.copy()
                else:
                    own_results_df = pd.concat([own_results_df, own_results_interim]).copy().reset_index(drop = True)

            trans_results_df.to_csv(save_location + economy + '_other_transformation_' + scenario + '_' + timestamp + '.csv', index = False)
            own_results_df.to_csv(save_location + economy + '_other_own_use_' + scenario + '_' + timestamp + '.csv', index = False)

