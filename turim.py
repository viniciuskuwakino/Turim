import pandas as pd

path_exp = '/home/kuak/Downloads/EXP_2022.csv'
path_imp = '/home/kuak/Downloads/IMP_2022.csv'

exp_df = pd.read_csv(path_exp, sep=";")
imp_df = pd.read_csv(path_imp, sep=";")

exp_df = exp_df.drop(['CO_ANO', 'CO_UNID', 'CO_PAIS', 'CO_VIA', 'CO_URF', 'QT_ESTAT', 'KG_LIQUIDO'], axis=1)
imp_df = imp_df.drop(['CO_ANO', 'CO_UNID', 'CO_PAIS', 'CO_VIA', 'CO_URF', 'QT_ESTAT', 'KG_LIQUIDO','VL_FRETE','VL_SEGURO'], axis=1)

exp_df = exp_df.sort_values(by=['SG_UF_NCM', 'CO_MES'])
imp_df = imp_df.sort_values(by=['SG_UF_NCM', 'CO_MES'])

ufs_dfs_exp = {uf: uf_df for uf, uf_df in exp_df.groupby('SG_UF_NCM')}
ufs_dfs_imp = {uf: uf_df for uf, uf_df in imp_df.groupby('SG_UF_NCM')}

meses = {1: 'jan', 2: 'fev', 3: 'mar', 4: 'abr', 5: 'mai', 6: 'jun', 7: 'jul', 8: 'ago', 9: 'set', 10: 'out', 11: 'nov', 12: 'dez'}
infos = ['Exp', 'Imp', 'Net']
colunas = list()
dfs = dict()

for k, v in meses.items():
    for i in infos:
        colunas.append(f"{i}_{v}")

for k in ufs_dfs_exp.keys():
    dfs[f"{k}"] = pd.DataFrame(columns=colunas)

for k in ufs_dfs_imp.keys():
    if k not in dfs:
        dfs[f"{k}"] = pd.DataFrame(columns=colunas)

        
for v, uf_df in ufs_dfs_exp.items():
    for mes, mes_df in uf_df.groupby('CO_MES'):
        uf_df = mes_df.groupby('CO_NCM')['VL_FOB'].sum()
        dfs[f"{v}"][f"Exp_{meses[mes]}"] = uf_df
        
for v, uf_df in ufs_dfs_imp.items():
    for mes, mes_df in uf_df.groupby('CO_MES'):
        uf_df = mes_df.groupby('CO_NCM')['VL_FOB'].sum()
        dfs[f"{v}"][f"Imp_{meses[mes]}"] = uf_df
        
for i in dfs.keys():
    dfs[i] = dfs[i].fillna(0)
    
for uf, df_uf in dfs.items():
    for k, v in meses.items():
        df_uf[f"{infos[2]}_{v}"] = df_uf[f"{infos[0]}_{v}"] - df_uf[f"{infos[1]}_{v}"]
    
    exps = df_uf.filter(like='Exp_')
    imps = df_uf.filter(like='Imp_')

    df_uf['Exp_total'] = exps.sum(axis=1)
    df_uf['Imp_total'] = imps.sum(axis=1)
    df_uf['Net_total'] = df_uf['Exp_total'] - df_uf['Imp_total']

[dfs[i].to_csv(f"{i}.csv") for i in dfs.keys()]