import pandas as pd

# Funções

def identifica_cdu(linha):
    if linha in range(100, 200):
        return "Filosofia e psicologia."
    elif linha in range(200, 300):
        return "Religião."
    elif linha in range(300, 400):
        return "Ciências sociais."
    elif linha in range(500, 600):
        return "Matemática e ciências naturais."
    elif linha in range(600, 700):
        return "Ciências aplicadas."
    elif linha in range(700, 800):
        return "Belas artes."
    elif linha in range(800, 900):
        return "Linguagem. Língua. Linguística."
    elif linha in range(900, 1000):
        return "Geografia. Biografia. História."

# Leitura dos dados de empréstimo
print("Iniciando leitura emprestimo")
dados1 = pd.read_csv('https://raw.githubusercontent.com/FranciscoFoz/7_Days_of_Code_Alura-Python-Pandas/main/Dia_1-Importando_dados/Datasets/dados_emprestimos/emprestimos-20101.csv')
dados2 = pd.read_csv('https://raw.githubusercontent.com/FranciscoFoz/7_Days_of_Code_Alura-Python-Pandas/main/Dia_1-Importando_dados/Datasets/dados_emprestimos/emprestimos-20102.csv')
dados3 = pd.read_csv('https://raw.githubusercontent.com/FranciscoFoz/7_Days_of_Code_Alura-Python-Pandas/main/Dia_1-Importando_dados/Datasets/dados_emprestimos/emprestimos-20111.csv')
dados4 = pd.read_csv('https://raw.githubusercontent.com/FranciscoFoz/7_Days_of_Code_Alura-Python-Pandas/main/Dia_1-Importando_dados/Datasets/dados_emprestimos/emprestimos-20121.csv')
dados5 = pd.read_csv('https://raw.githubusercontent.com/FranciscoFoz/7_Days_of_Code_Alura-Python-Pandas/main/Dia_1-Importando_dados/Datasets/dados_emprestimos/emprestimos-20131.csv')
dados6 = pd.read_csv('https://raw.githubusercontent.com/FranciscoFoz/7_Days_of_Code_Alura-Python-Pandas/main/Dia_1-Importando_dados/Datasets/dados_emprestimos/emprestimos-20132.csv')
dados7 = pd.read_csv('https://raw.githubusercontent.com/FranciscoFoz/7_Days_of_Code_Alura-Python-Pandas/main/Dia_1-Importando_dados/Datasets/dados_emprestimos/emprestimos-20141.csv')
dados8 = pd.read_csv('https://raw.githubusercontent.com/FranciscoFoz/7_Days_of_Code_Alura-Python-Pandas/main/Dia_1-Importando_dados/Datasets/dados_emprestimos/emprestimos-20142.csv')
dados9 = pd.read_csv('https://raw.githubusercontent.com/FranciscoFoz/7_Days_of_Code_Alura-Python-Pandas/main/Dia_1-Importando_dados/Datasets/dados_emprestimos/emprestimos-20151.csv')
dados10 = pd.read_csv('https://raw.githubusercontent.com/FranciscoFoz/7_Days_of_Code_Alura-Python-Pandas/main/Dia_1-Importando_dados/Datasets/dados_emprestimos/emprestimos-20152.csv')
dados11 = pd.read_csv('https://raw.githubusercontent.com/FranciscoFoz/7_Days_of_Code_Alura-Python-Pandas/main/Dia_1-Importando_dados/Datasets/dados_emprestimos/emprestimos-20161.csv')
dados12 = pd.read_csv('https://raw.githubusercontent.com/FranciscoFoz/7_Days_of_Code_Alura-Python-Pandas/main/Dia_1-Importando_dados/Datasets/dados_emprestimos/emprestimos-20162.csv')
dados13 = pd.read_csv('https://raw.githubusercontent.com/FranciscoFoz/7_Days_of_Code_Alura-Python-Pandas/main/Dia_1-Importando_dados/Datasets/dados_emprestimos/emprestimos-20171.csv')
dados14 = pd.read_csv('https://raw.githubusercontent.com/FranciscoFoz/7_Days_of_Code_Alura-Python-Pandas/main/Dia_1-Importando_dados/Datasets/dados_emprestimos/emprestimos-20172.csv')
dados15 = pd.read_csv('https://raw.githubusercontent.com/FranciscoFoz/7_Days_of_Code_Alura-Python-Pandas/main/Dia_1-Importando_dados/Datasets/dados_emprestimos/emprestimos-20181.csv')
dados16 = pd.read_csv('https://raw.githubusercontent.com/FranciscoFoz/7_Days_of_Code_Alura-Python-Pandas/main/Dia_1-Importando_dados/Datasets/dados_emprestimos/emprestimos-20182.csv')
dados17 = pd.read_csv('https://raw.githubusercontent.com/FranciscoFoz/7_Days_of_Code_Alura-Python-Pandas/main/Dia_1-Importando_dados/Datasets/dados_emprestimos/emprestimos-20191.csv')
dados18= pd.read_csv('https://raw.githubusercontent.com/FranciscoFoz/7_Days_of_Code_Alura-Python-Pandas/main/Dia_1-Importando_dados/Datasets/dados_emprestimos/emprestimos-20192.csv')
dados19 = pd.read_csv('https://raw.githubusercontent.com/FranciscoFoz/7_Days_of_Code_Alura-Python-Pandas/main/Dia_1-Importando_dados/Datasets/dados_emprestimos/emprestimos-20201.csv')
emprestimos_biblioteca = pd.concat([dados1,dados2,dados3,dados4,dados5,dados6,dados7,dados8,dados9,dados10,dados11,dados12,dados13,dados14,dados15,dados16,dados17,dados18,dados19],ignore_index=True)
print("Pronto")
# Remocao de duplicatas

emprestimos_biblioteca = emprestimos_biblioteca.drop_duplicates()
print("Dados unificados")
# Dados dos livros (exemplares)
print("Lendo dados exemplares")
exemplares = pd.read_parquet('https://github.com/FranciscoFoz/7_Days_of_Code_Alura-Python-Pandas/raw/main/Dia_1-Importando_dados/Datasets/dados_exemplares.parquet')

# União dos dados
print("Concluído")
emprestimos = emprestimos_biblioteca.merge(exemplares)

# Salvar os dados para uso posterior e análise
emprestimos.to_parquet(path="../dados_raw/emprestimos.parquet")
print("Todas as informações forma unificadas e salvas")

# Iniciando segundo processo de data cleaning and formatting

#emprestimos = pd.read_parquet('../dados_raw/emprestimos.parquet')

# Aplicar a função que cria a coluna baseada na localização dos dados 
emprestimos['Nome CDU'] = emprestimos['localizacao'].apply(identifica_cdu)

# Remove a coluna não usada
emprestimos = emprestimos.drop(labels='registro_sistema',axis=1)

emprestimos['matricula_ou_siape'] = emprestimos['matricula_ou_siape'].astype('str')