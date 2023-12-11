# Pipeline energy consumption (even though it's within the demand block, it's only modelled after consumption of gas is determined within TFC and power)

# 07_petroleum_products
# 08_gas
# 17_electricity

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
# APEC_economies = APEC_economies[18:19]

# 2021 and beyond
proj_years = list(range(2021, 2071, 1))
proj_years_str = [str(i) for i in proj_years]

latest_hist = '2020'
ref_elec = 0.002
tgt_elec = 0.004
switch_start_year = '2025'

# latest EGEDA data
EGEDA_df = pd.read_csv(latest_EGEDA)

# Pipeline fuels
relevant_fuels = ['07_petroleum_products', '08_gas', '17_electricity', '19_total']

# EGEDA pipeline data
EGEDA_pipe = EGEDA_df[(EGEDA_df['sub1sectors'] == '15_05_pipeline_transport') &
                      (EGEDA_df['fuels'].isin(relevant_fuels)) &
                      (EGEDA_df['subfuels'] == 'x')].copy().reset_index(drop = True)

for economy in APEC_economies:
    # Save location
    save_location = './results/03_pipeline_transport/{}/'.format(economy)

    if not os.path.isdir(save_location):
        os.makedirs(save_location)

    # This is the location where the merged TFC and transformation results are provided
    modelled_result = './data/copy 02_TFC here/'

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

        # Pipeline transport results needed
        EGEDA_pipe_ref = EGEDA_pipe[EGEDA_pipe['economy'] == economy].copy().reset_index(drop = True)
        EGEDA_pipe_tgt = EGEDA_pipe[EGEDA_pipe['economy'] == economy].copy().reset_index(drop = True)
        EGEDA_pipe_tgt['scenarios'] = 'target'

        # Scenario dictionary with relevant pieces to use later
        scenario_dict = {'ref': [results_ref, EGEDA_pipe_ref, ref_elec],
                         'tgt': [results_tgt, EGEDA_pipe_tgt, tgt_elec]}
        
        for scenario in scenario_dict.keys():
            # Data frame with results from other sectors to use to build trajectories to fill the trans and own df's
            tfc_df = scenario_dict[scenario][0]

            tfc_df = tfc_df[(tfc_df['sectors'].isin(['12_total_final_consumption'])) &
                            (tfc_df['sub1sectors'] == 'x') &
                            (tfc_df['fuels'] == '08_gas') &
                            (tfc_df['subfuels'] == 'x')].copy().reset_index(drop = True)
            
            # Fill NA so they're zeroes instead
            tfc_df = tfc_df.fillna(0)

            # Sum consumption (TFC and transformation)
            tfc_df = tfc_df.groupby(['scenarios', 'economy', 'sub1sectors', 'sub2sectors', 'sub3sectors', 'sub4sectors', 'subfuels', 'fuels'])\
                    .sum().reset_index().assign(sectors = 'tfc')

            # Dataframes to populate
            pipe_df = scenario_dict[scenario][1]
            pipe_df = pipe_df.fillna(0)
        
            # Define ratio dataframe
            ratio_df = pd.DataFrame(columns = ['fuels', '2020'] + proj_years_str)
            ratio_df.loc[0, 'fuels'] = '07_petroleum_products'
            ratio_df.loc[1, 'fuels'] = '08_gas'
            ratio_df.loc[2, 'fuels'] = '17_electricity'

            # Define ratio in most recent historical
            for year in ['2020'] + proj_years_str:
                for fuel in relevant_fuels[:-1]:
                    if pipe_df.loc[pipe_df['fuels'] == '19_total', latest_hist].values[0] == 0:
                        ratio_df.loc[ratio_df['fuels'] == fuel, year] = 0

                    else:
                        ratio_df.loc[ratio_df['fuels'] == fuel, year] = pipe_df.loc[pipe_df['fuels'] == fuel, latest_hist].values[0] /\
                            pipe_df.loc[pipe_df['fuels'] == '19_total', latest_hist].values[0]
            
            pipe_df = pipe_df.drop([3]).copy().reset_index(drop = True)

            # Incorporate fuel switching
            int_start = int(proj_years_str[0])
            fs_full = scenario_dict[scenario][2]
            fs_half = fs_full / 2
            
            for year in proj_years_str:
                int_year = int(year)
                if ratio_df.loc[ratio_df['fuels'] == '17_electricity', str(int(year) - 1)].values[0] <= (1 - fs_full):
                    ratio_df.loc[ratio_df['fuels'] == '17_electricity', year] = ratio_df.loc[ratio_df['fuels'] == '17_electricity', str(int(year) - 1)].values[0] + fs_full
                else:
                    ratio_df.loc[ratio_df['fuels'] == '17_electricity', year] = 1
                
                if (ratio_df.loc[ratio_df['fuels'] == '07_petroleum_products', str(int(year) - 1)].values[0] >= fs_half) & \
                    (ratio_df.loc[ratio_df['fuels'] == '08_gas', str(int(year) - 1)].values[0] >= fs_half):
                    
                    ratio_df.loc[ratio_df['fuels'] == '07_petroleum_products', year] = ratio_df.loc[ratio_df['fuels'] == '07_petroleum_products', str(int(year) - 1)].values[0] - fs_half
                    ratio_df.loc[ratio_df['fuels'] == '08_gas', year] = ratio_df.loc[ratio_df['fuels'] == '08_gas', str(int(year) - 1)].values[0] - fs_half
                
                if ratio_df.loc[ratio_df['fuels'] == '07_petroleum_products', str(int(year) - 1)].values[0] < fs_half:
                    ratio_df.loc[ratio_df['fuels'] == '07_petroleum_products', year] = 0

                if ratio_df.loc[ratio_df['fuels'] == '08_gas', str(int(year) - 1)].values[0] < fs_half:
                    ratio_df.loc[ratio_df['fuels'] == '08_gas', year] = 0

                if (ratio_df.loc[ratio_df['fuels'] == '07_petroleum_products', str(int(year) - 1)].values[0] >= fs_full) & \
                    (ratio_df.loc[ratio_df['fuels'] == '08_gas', str(int(year) - 1)].values[0] == 0):
                    ratio_df.loc[ratio_df['fuels'] == '07_petroleum_products', year] = ratio_df.loc[ratio_df['fuels'] == '07_petroleum_products', str(int(year) - 1)].values[0] - fs_full

                if (ratio_df.loc[ratio_df['fuels'] == '07_petroleum_products', str(int(year) - 1)].values[0] == 0) & \
                    (ratio_df.loc[ratio_df['fuels'] == '08_gas', str(int(year) - 1)].values[0] >= fs_full):
                    ratio_df.loc[ratio_df['fuels'] == '08_gas', year] = ratio_df.loc[ratio_df['fuels'] == '08_gas', str(int(year) - 1)].values[0] - fs_full 

            # Now populate the pipeline dataframe
            for year in proj_years_str:
                if tfc_df.loc[0, str(int(year) - 1)] == 0:
                    ratio = 0

                else:
                    ratio = tfc_df.loc[0, year] / tfc_df.loc[0, str(int(year) - 1)]

                for fuel in relevant_fuels[:-1]:
                    pipe_df.loc[pipe_df['fuels'] == fuel, year] = pipe_df.loc[:, str(int(year) - 1)].sum() * ratio * ratio_df.loc[ratio_df['fuels'] == fuel, year].values[0]

            pipe_df.to_csv(save_location + economy + '_pipeline_transport_' + scenario + '_' + timestamp + '.csv', index = False)

