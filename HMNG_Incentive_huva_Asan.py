# main.py
from Insentive_Final_dump_2 import Insentive_Final_dump
from DisputeAdd_3 import process_csv
from insentive_Report_4 import incentive_Report
from Seperate_Role_incentive_5 import split_role
from RefundCalculations_6 import refund
from Final_Report_7 import Final_Report
from Whatsapp_MSG_File_Generate_8 import whatsapp_msg


def main():

    print("Fetching final dump from the database...")
    Insentive_Final_dump()

    print("Adding disputes to the final dump...")
    process_csv()

    print("Generating final dump with team assignments...")
    incentive_Report()

    print("Dividing data by role...")
    split_role()

    print("Removing refunds from the data...")
    refund()

    print("Generating the final report...")
    Final_Report()

    print("Generating WhatsApp message data...")
    whatsapp_msg()


if __name__ == "__main__":
    main()
