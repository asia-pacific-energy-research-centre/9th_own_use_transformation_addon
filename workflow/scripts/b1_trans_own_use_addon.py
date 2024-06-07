# Modelling 9_transformation and 10_own_use_and_losses that have not been modelled by other sectors
# Late addition to this script is non-specified demand
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

for index, item in enumerate(APEC_economies):
    if (item == '17_SIN'):
        APEC_economies[index] = '17_SGP'
    elif (item == '15_RP'):
        APEC_economies[index] = '15_PHL'

APEC_economies = APEC_economies[14:15]

# 2022 and beyond
proj_years = list(range(2022, 2071, 1))
proj_years_str = [str(i) for i in proj_years]

# latest EGEDA data
EGEDA_df = pd.read_csv(latest_EGEDA)
EGEDA_df = EGEDA_df.drop(columns = ['is_subtotal']).copy().reset_index(drop = True)

EGEDA_df[EGEDA_df['sub1sectors'].str.startswith('09')]['sub1sectors'].unique()
EGEDA_df[EGEDA_df['sub1sectors'].str.startswith('10')]['sub2sectors'].unique()
EGEDA_df[EGEDA_df['sub1sectors'].str.startswith('16')]['sub1sectors'].unique()

# sub1sectors transformation categories that need to be modelled
trans_df = pd.read_csv('./data/config/transformation_orphans.csv', header = None)
trans_cats = trans_df[0].values.tolist()

# sub2sectors own-use categories that need to be modelled
ownuse_df = pd.read_csv('./data/config/ownuse_cats.csv', header = None)
ownuse_cats = ownuse_df[0].values.tolist()

# Non-specified end-use
nonspec_df = pd.read_csv('./data/config/other_nonspec.csv', header = None)
nonspec_cats = nonspec_df[0].values.tolist()

relevant_fuels = ['01_coal', '02_coal_products', '06_crude_oil_and_ngl', '07_petroleum_products',
                  '08_gas', '15_solid_biomass', '16_others', '17_electricity', '18_heat']

EGEDA_trans = EGEDA_df[EGEDA_df['sub1sectors'].isin(trans_cats)].copy().reset_index(drop = True)
EGEDA_own = EGEDA_df[EGEDA_df['sub2sectors'].isin(ownuse_cats)].copy().reset_index(drop = True)
EGEDA_nonspec = EGEDA_df[EGEDA_df['sub1sectors'].isin(nonspec_cats)].copy().reset_index(drop = True)

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

        # Non-specified needed
        EGEDA_nonspec_ref = EGEDA_nonspec[EGEDA_nonspec['economy'] == economy].copy().reset_index(drop = True)
        EGEDA_nonspec_tgt = EGEDA_nonspec[EGEDA_nonspec['economy'] == economy].copy().reset_index(drop = True)
        EGEDA_nonspec_tgt['scenarios'] = 'target'

        scenario_dict = {'ref': [results_ref, EGEDA_trans_ref, EGEDA_own_ref, EGEDA_nonspec_ref],
                         'tgt': [results_tgt, EGEDA_trans_tgt, EGEDA_own_tgt, EGEDA_nonspec_tgt]}
        
        for scenario in scenario_dict.keys():
            
            scenario_df = scenario_dict[scenario][0].copy()
            
            tfc_df = scenario_df[(scenario_df['sectors'].isin(['12_total_final_consumption'])) & (scenario_df['fuels'].isin(relevant_fuels)) & (scenario_df['subtotal_results'] == True)].copy().reset_index(drop = True)
            ########
            #and minus the pipeline and non specified use from the gas consumption in case it is in the folder, so got merged in with the other demand data
            pipeline_nonspecified_use = scenario_df[(scenario_df['sub1sectors'].isin(['15_05_pipeline_transport', '16_05_nonspecified_others'])) & (scenario_df['fuels'].isin(relevant_fuels)) & (scenario_df['subtotal_results'] == False)].copy().reset_index(drop = True)
            if len(pipeline_nonspecified_use) > 0:
                #sum pipeline use and nonspecified others
                pipeline_nonspecified_use = pipeline_nonspecified_use.groupby(['scenarios','economy','fuels']).sum().reset_index()
                #double check the two dataframes are 1 row each and then subtract the pipeline use from the gas consumption
                if len(tfc_df) != len(pipeline_nonspecified_use):
                    raise Exception('The consumption dataframes are not the expected length')
                tfc_df[proj_years] = tfc_df[proj_years] - pipeline_nonspecified_use[proj_years]
            ########

            tfc_df = tfc_df.fillna(0)
            
            # Dataframes to populate
            trans_df = scenario_dict[scenario][1]
            trans_df = trans_df.fillna(0)
            own_df = scenario_dict[scenario][2]
            own_df = own_df.fillna(0)
            nons_df = scenario_dict[scenario][3]
            nons_df = nons_df.fillna(0)

            # Define results dataframe
            trans_results_df = pd.DataFrame(columns = trans_df.columns)
            own_results_df = pd.DataFrame(columns = own_df.columns)
            nons_results_df = pd.DataFrame(columns = nons_df.columns)

            for fuel in tfc_df['fuels'].unique():
                fuel_agg_row = tfc_df[tfc_df['fuels'] == fuel].copy().reset_index(drop = True)

                trans_results_interim = trans_df[trans_df['fuels'] == fuel].copy().reset_index(drop = True)
                own_results_interim = own_df[own_df['fuels'] == fuel].copy().reset_index(drop = True)
                nons_results_interim = nons_df[nons_df['fuels'] == fuel].copy().reset_index(drop = True)

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

                    for row in nons_results_interim.index:
                        nons_results_interim.loc[row, year] = nons_results_interim.loc[row, str(int(year) - 1)] * ratio

                if trans_results_df.empty:
                    trans_results_df = trans_results_interim.copy()
                else:             
                    trans_results_df = pd.concat([trans_results_df, trans_results_interim]).copy().reset_index(drop = True)
                
                if own_results_df.empty:
                    own_results_df = own_results_interim.copy()
                else:
                    own_results_df = pd.concat([own_results_df, own_results_interim]).copy().reset_index(drop = True)

                if nons_results_df.empty:
                    nons_results_df = nons_results_interim.copy()
                else:
                    nons_results_df = pd.concat([nons_results_df, nons_results_interim]).copy().reset_index(drop = True)

            trans_results_df.to_csv(save_location + economy + '_other_transformation_' + scenario + '_' + timestamp + '.csv', index = False)
            own_results_df.to_csv(save_location + economy + '_other_own_use_' + scenario + '_' + timestamp + '.csv', index = False)
            nons_results_df.to_csv(save_location + economy + '_non_specified_' + scenario + '_' + timestamp + '.csv', index = False)

