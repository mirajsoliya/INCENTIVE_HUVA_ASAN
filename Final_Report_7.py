import pandas as pd

def split_and_save_reports(bdm_report_file, PSA_report_file, bdm_qualified_threshold=100000, bdm_cases_threshold=1, PSA_qualified_threshold=25000, PSA_cases_threshold=4):
# def split_and_save_reports(bdm_report_file, bdm_qualified_threshold=100000, bdm_cases_threshold=3):
    # Load the final incentive reports
    bdm_report = pd.read_csv(bdm_report_file)
    PSA_report = pd.read_csv(PSA_report_file)

    
    if 'Role' in bdm_report.columns:
        bdm_report = bdm_report.drop(columns=['Role'])
    if 'Role' in PSA_report.columns:
        PSA_report = PSA_report.drop(columns=['Role'])

    # Filter for Qualified and Not Qualified BDMs
    qualified_bdm = bdm_report[(bdm_report['Total Collection'] >= bdm_qualified_threshold) & (bdm_report['Cases'] >= bdm_cases_threshold)]
    not_qualified_bdm = bdm_report[(bdm_report['Total Collection'] < bdm_qualified_threshold) | (bdm_report['Cases'] < bdm_cases_threshold)]

    # Filter for Qualified and Not Qualified TLs

    qualified_PSA = PSA_report[(PSA_report['Total Collection'] >= PSA_qualified_threshold) & (PSA_report['Cases'] >= PSA_cases_threshold)]
    not_qualified_PSA = PSA_report[(PSA_report['Total Collection'] < PSA_qualified_threshold) | (PSA_report['Cases'] < PSA_cases_threshold)]

    # Save the filtered DataFrames to CSV files without the 'Role' column
    qualified_bdm.to_csv(f'Qualified_BDM.csv', index=False)
    not_qualified_bdm.to_csv(f'Not_Qualified_BDM.csv', index=False)

    qualified_PSA.to_csv(f'Qualified_PSA.csv', index=False)
    not_qualified_PSA.to_csv(f'Not_Qualified_PSA.csv', index=False)

    print("Reports divided and saved as:")
    print(f"- Qualified_BDM.csv")
    print(f"- Not_Qualified_BDM.csv")
    print(f"- Qualified_PSA.csv")
    print(f"- Not_Qualified_PSA.csv")

def Final_Report():
    split_and_save_reports('BDM_Final_incentive_report.csv', 'PSA_Final_incentive_report.csv')
    # split_and_save_reports('BDM_Final_incentive_report.csv')


# if __name__ == "__main__":
#     Final_Report()