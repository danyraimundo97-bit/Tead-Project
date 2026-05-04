import os
import requests
import pandas as pd

def fetch_and_filter_opencellid(api_token: str, output_filename: str = "opencellid_pt.csv"):
    """
    Faz o download do dataset global do OpenCelliD e filtra apenas as antenas de Portugal (MCC 268).
    """
    url = f"https://opencellid.org/ocid/downloads?token={api_token}&type=full&file=cell_towers.csv.gz"
    temp_gz_file = "temp_cell_towers.csv.gz"
    
    # 1. Download do ficheiro (Streaming)
    print("A iniciar o download do OpenCelliD (pode demorar alguns minutos dependendo da net)...")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(temp_gz_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
    
    print("Download concluído! A extrair e filtrar as antenas de Portugal...")
    
    # 2. Filtragem eficiente (chunking)
    chunk_size = 500000 
    col_names = ["radio", "mcc", "net", "area", "cell", "unit", "lon", "lat", "range", "samples", "changeable", "created", "updated", "averageSignal"]
    
    first_chunk = True
    total_linhas_pt = 0
    
    for chunk in pd.read_csv(temp_gz_file, compression='gzip', header=None, names=col_names, chunksize=chunk_size, on_bad_lines='skip'):
        # Filtra apenas o MCC de Portugal (268)
        df_pt = chunk[chunk['mcc'] == 268]
        
        if not df_pt.empty:
            df_pt.to_csv(output_filename, mode='a', header=first_chunk, index=False)
            first_chunk = False
            total_linhas_pt += len(df_pt)
            
    # 3. Limpeza
    if os.path.exists(temp_gz_file):
        os.remove(temp_gz_file)
        
    print(f"Sucesso! Ficheiro '{output_filename}' criado com {total_linhas_pt} antenas portuguesas.")

# --- Execução ---
if __name__ == "__main__":
    MEU_TOKEN = "pk.320904680f47693cd930f11513e0e6ac"
    fetch_and_filter_opencellid(api_token=MEU_TOKEN)