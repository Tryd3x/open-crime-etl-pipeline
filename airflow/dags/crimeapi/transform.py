import pandas as pd
import logging

logger = logging.getLogger(__name__)

def transform(data):
        df = pd.DataFrame(data)

        col_drop = [
            ':@computed_region_awaf_s7ux',
            ':@computed_region_6mkv_f3dw',
            ':@computed_region_vrxf_vc4k',
            ':@computed_region_bdys_3d7i',
            ':@computed_region_43wa_7qmu',
            ':@computed_region_rpca_8um6',
            ':@computed_region_d9mm_jgwp',
            ':@computed_region_d3ds_rm58',
            ':@computed_region_8hcu_yrd4',
            'location',
            ':id',
            ':version',
            ':created_at',
            'year',
            'updated_on'
        ]
        
        rename_col = {
            'id' : 'crime_id',
            'case_number' : 'case',
            'date' : 'date_of_occurrence',
            'primary_type' : 'primary_description',
            'description' : 'secondary_description',
            ':updated_at' : 'source_updated_on'
        }
        
        # Drop
        # logger.info(f"Dropping columns: {col_drop}")
        df.drop(columns=col_drop, inplace=True)

        # Rename
        # logger.info(f"Renaming columns: {rename_col}")
        df.rename(columns=rename_col, inplace=True)

        # Handle Null
        # logger.info(f"Replacing NaN -> None")
        df = df.where(pd.notnull(df), None)

        return df