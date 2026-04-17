import os
import zipfile
import polars as pl
import sys
from datetime import datetime

DGT_FIELDS = [
    [1, 'FEC_MATRICULA', 8], [2, 'COD_CLASE_MAT', 1], [3, 'FEC_TRAMITACION', 8], 
    [4, 'MARCA_ITV', 30], [5, 'MODELO_ITV', 22], [6, 'COD_PROCEDENCIA_ITV', 1], 
    [7, 'BASTIDOR_ITV', 21], [8, 'COD_TIPO', 2], [9, 'COD_PROPULSION_ITV', 1], 
    [10, 'CILINDRADA_ITV', 5], [11, 'POTENCIA_ITV', 6], [12, 'TARA', 6], 
    [13, 'PESO_MAX', 6], [14, 'NUM_PLAZAS', 3], [15, 'IND_PRECINTO', 2], 
    [16, 'IND_EMBARGO', 2], [17, 'NUM_TRANSMISIONES', 2], [18, 'NUM_TITULARES', 2], 
    [19, 'LOCALIDAD_VEHICULO', 24], [20, 'COD_PROVINCIA_VEH', 2], [21, 'COD_PROVINCIA_MAT', 2], 
    [22, 'CLAVE_TRAMITE', 1], [23, 'FEC_TRAMITE', 8], [24, 'CODIGO_POSTAL', 5], 
    [25, 'FEC_PRIM_MATRICULACION', 8], [26, 'IND_NUEVO_USADO', 1], [27, 'PERSONA_FISICA_JURIDICA', 1], 
    [28, 'CODIGO_ITV', 9], [29, 'SERVICIO', 3], [30, 'COD_MUNICIPIO_INE_VEH', 5], 
    [31, 'MUNICIPIO', 30], [32, 'KW_ITV', 7], [33, 'NUM_PLAZAS_MAX', 3], 
    [34, 'CO2_ITV', 5], [35, 'RENTING', 1], [36, 'COD_TUTELA', 1], 
    [37, 'COD_POSESION', 1], [38, 'IND_BAJA_DEF', 1], [39, 'IND_BAJA_TEMP', 1], 
    [40, 'IND_SUSTRACCION', 1], [41, 'BAJA_TELEMATICA', 11], [42, 'TIPO_ITV', 25], 
    [43, 'VARIANTE_ITV', 25], [44, 'VERSION_ITV', 35], [45, 'FABRICANTE_ITV', 70], 
    [46, 'MASA_ORDEN_MARCHA_ITV', 6], [47, 'MASA_MÁXIMA_TECNICA_ADMISIBLE_ITV', 6], 
    [48, 'CATEGORÍA_HOMOLOGACIÓN_EUROPEA_ITV', 4], [49, 'CARROCERIA', 4], [50, 'PLAZAS_PIE', 3], 
    [51, 'NIVEL_EMISIONES_EURO_ITV', 8], [52, 'CONSUMO', 4], [53, 'CLASIFICACIÓN_REGLAMENTO_VEHICULOS_IT', 4], 
    [54, 'CATEGORÍA_VEHÍCULO_ELÉCTRICO', 4], [55, 'AUTONOMÍA_VEHÍCULO_ELÉCTRICO', 6], 
    [56, 'MARCA_VEHÍCULO_BASE', 30], [57, 'FABRICANTE_VEHÍCULO_BASE', 50], [58, 'TIPO_VEHÍCULO_BASE', 35], 
    [59, 'VARIANTE_VEHÍCULO_BASE', 25], [60, 'VERSIÓN_VEHÍCULO_BASE', 35], [61, 'DISTANCIA_EJES_12_ITV', 4], 
    [62, 'VIA_ANTERIOR_ITV', 4], [63, 'VIA_POSTERIOR_ITV', 4], [64, 'TIPO_ALIMENTACION_ITV', 1], 
    [65, 'CONTRASEÑA_HOMOLOGACION_ITV', 25], [66, 'ECO_INNOVACION_ITV', 1], [67, 'REDUCCION_ECO_ITV', 4], 
    [68, 'CODIGO_ECO_ITV', 25], [69, 'FEC_PROCESO', 8]
]

def main(
    raw_dir="data/raw/vehicle_registrations",
    output_path="data/standardized/vehicle_registrations.parquet",
    **kwargs
):
    """
    Standardizes vehicle registrations with strict filters and propulsion mapping.
    """
    print(f"🚀 Standardizing Vehicle Registrations from {raw_dir}...")
    
    if not os.path.exists(raw_dir):
        print(f"Error: Directory {raw_dir} not found.")
        sys.exit(1)

    zip_files = sorted([f for f in os.listdir(raw_dir) if f.endswith('.zip')])
    if not zip_files:
        print(f"Error: No ZIP files found in {raw_dir}")
        sys.exit(1)

    # Prepare col specs
    col_names = [f[1] for f in DGT_FIELDS]
    col_specs = []
    curr = 0
    for f in DGT_FIELDS:
        length = f[2]
        col_specs.append((curr, curr + length))
        curr += length

    columns_to_extract = ["FEC_MATRICULA", "MARCA_ITV", "COD_TIPO", "COD_PROPULSION_ITV", "CLAVE_TRAMITE"]

    cod_propulsion = {
        "0":"Gasolina", "1":"Diesel", "2":"Eléctrico", "3":"Otros", "4":"Butano", 
        "5":"Solar", "6":"Gas Licuado de Petróleo", "7":"Gas Natural Comprimido", 
        "8":"Gas Natural Licuado", "9":"Hidrógeno", "A":"Biometano", "B":"Etanol", "C":"Biodiesel"
    }

    all_dfs = []
    print(f" - Unzipping and parsing {len(zip_files)} files...")
    
    for zf in zip_files:
        zpath = os.path.join(raw_dir, zf)
        try:
            with zipfile.ZipFile(zpath, 'r') as z:
                internal_name = z.namelist()[0]
                with z.open(internal_name) as f:
                    lines = f.read().decode('latin-1').splitlines()[1:]
                    rows = []
                    for line in lines:
                        if not line.strip(): continue
                        row = [line[s:e].strip() for s, e in col_specs]
                        rows.append(row)
                    
                    if rows:
                        df = pl.DataFrame(rows, schema=col_names, orient="row")
                        df = df.select(columns_to_extract)
                        
                        # Apply Filtering: COD_TIPO == '40' and CLAVE_TRAMITE in ['1', '5', 'B']
                        df = df.filter(
                            (pl.col("COD_TIPO") == "40") & 
                            (pl.col("CLAVE_TRAMITE").is_in(["1", "5", "B"]))
                        )
                        
                        if df.is_empty(): continue
                        
                        # Date Transformation
                        df = df.with_columns(
                            pl.col("FEC_MATRICULA").str.strptime(pl.Date, format="%d%m%Y", strict=False)
                        ).rename({"FEC_MATRICULA": "date"})
                        
                        # Propulsion Mapping
                        df = df.with_columns(
                            pl.col("COD_PROPULSION_ITV").replace(cod_propulsion, default=None)
                        ).filter(pl.col("COD_PROPULSION_ITV").is_not_null())
                        
                        # Rename columns to English
                        df = df.rename({
                            "MARCA_ITV": "brand",
                            "COD_PROPULSION_ITV": "propulsion"
                        })
                        
                        # Final selection
                        df = df.select(["date", "brand", "propulsion"])
                        
                        all_dfs.append(df)
        except Exception as e:
            print(f"   - Error processing {zf}: {e}")

    if not all_dfs:
        print("Error: No data matched the filters.")
        sys.exit(1)

    print(" - Merging and saving output...")
    final_df = pl.concat(all_dfs)
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    final_df.write_parquet(output_path)
    
    print(f"✨ SUCCESS: Vehicle Registrations standardized ({len(final_df)} filtered records).")

if __name__ == "__main__":
    main()
