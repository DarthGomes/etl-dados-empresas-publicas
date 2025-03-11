import os
import polars as pl


def read_csv_safely(file_path, column_names=None, *index):
    try:
        df = pl.read_csv(
            file_path,
            separator=';',
            quote_char='"',
            encoding='ISO-8859-15',
            has_header=False,
            try_parse_dates=True,
            infer_schema_length=10000,
        )
        if column_names:
            df.columns = column_names[: len(df.columns)]
            return df
    except Exception as e:
        print(f'Erro ao ler arquivo {index}: {file_path}')
        print(f'Erro: {e}')
        return None


def harmonized_dtypes(df):
    return df.with_columns(
        [
            pl.col(df.columns[0]).cast(pl.Int64),
            pl.col(df.columns[1]).cast(pl.Utf8),
            pl.col(df.columns[2]).cast(pl.Int64),
            pl.col(df.columns[3]).cast(pl.Int64),
            pl.col(df.columns[4]).cast(pl.Utf8),
            pl.col(df.columns[5]).cast(pl.Utf8),
            pl.col(df.columns[6]).cast(pl.Utf8),
        ]
    )


class DataCleansing:
    def __init__(self):
        self.ventures = 'downloaded_files/empreendimentos'
        self.companies = 'downloaded_files/empresas'
        self.municipalities = 'downloaded_files/municipios'
        self.countries = 'downloaded_files/paises'
        self.cnaes = 'downloaded_files/cnae'
        self.ventures_columns = [
            'cnpj_basico',
            'cnpj_ordem',
            'cnpj_dv',
            'identificador_matriz_filial',
            'nome_fantasia',
            'situacao_cadastral',
            'data_situacao_cadastral',
            'motivo_situacao_cadastral',
            'nome_cidade_exterior',
            'pais',
            'data_inicio_atividade',
            'cnae_principal',
            'cnae_secundario',
            'tipo_logradouro',
            'logradouro',
            'numero',
            'complemento',
            'bairro',
            'cep',
            'uf',
            'municipio',
            'ddd_1',
            'telefone_1',
            'ddd_2',
            'telefone_2',
            'ddd_fax',
            'fax',
            'email',
            'situacao_especial',
            'data_situacao_especial',
        ]
        self.companies_columns = [
            'cnpj_basico',
            'razao_social',
            'natureza_juridica',
            'qualificacao_responsavel',
            'capital_social',
            'porte_da_empresa',
            'ente_federativo_responsavel',
        ]
        self.code_description = ['codigo', 'descricao']
        self.municipalities_columns = ['codigo', 'nome_municipio']
        self.countries_columns = ['codigo', 'nome_pais']

    def _concatenate_dataframes(self, dataframes: list):

        try:
            new_df = pl.concat(dataframes)
            # print(f'O tamanho do df final é {new_df.shape}')
            return new_df
        except Exception as e:
            print(print(f'Não foi possível concatenar os dataframes: {e}'))
            return None

    def companies_cleansing(self):
        dataframes = []
        for i in range(10):
            file_path = f'{os.path.abspath(self.companies)}/Empresas{i}.csv'
            df = read_csv_safely(file_path, self.companies_columns, i)
            if df is not None:
                # print(f'Dataframe {i} lido com sucesso. Shape: {df.shape}')
                dataframes.append(df)
            else:
                print(f'Dataframe {i} não pode ser lido')

            data = [harmonized_dtypes(df) for df in dataframes]

            # for i, df in enumerate(dataframes):
            # print(f'Tipos de dados do dataframe {i}: {df.dtypes}')

        new_df = self._concatenate_dataframes(data)

        df = new_df.with_columns(
            pl.col('capital_social')
            .str.replace(',', '.')
            .str.replace(r'[^\d.]', '')
            .cast(pl.Float64)
        )

        df = df.with_columns(
            [
                pl.when(pl.col('capital_social').is_null())
                .then(pl.lit(0))
                .otherwise(pl.col('capital_social').floor().cast(pl.Int64))
                .alias('capital_social_int')
            ]
        ).with_columns(
            [
                pl.when(pl.col('capital_social_int') <= 81000)
                .then(pl.lit('MEI'))
                .when(
                    (pl.col('capital_social_int') > 81000)
                    & (pl.col('capital_social_int') <= 360000)
                )
                .then(pl.lit('ME'))
                .when(
                    (pl.col('capital_social_int') > 360000)
                    & (pl.col('capital_social_int') <= 4800000)
                )
                .then(pl.lit('Empresa de Pequeno Porte'))
                .when(
                    (pl.col('capital_social_int') > 4800000)
                    & (pl.col('capital_social_int') <= 300000000)
                )
                .then(pl.lit('Empresa de Médio Porte'))
                .when(pl.col('capital_social_int') > 300000000)
                .then(pl.lit('Empresa de Grande Porte'))
                .otherwise(None)
                .alias('classificacao_porte')
            ]
        )

        new_df = df.filter(
            pl.col('classificacao_porte').is_in(
                [
                    'Empresa de Pequeno Porte',
                    'Empresa de Médio Porte',
                    'Empresa de Grande Porte',
                ]
            )
        )
        final_df = new_df.select(
            [
                'cnpj_basico',
                'razao_social',
                'capital_social',
                'classificacao_porte',
            ]
        )
        # print(final_df.shape)
        return pl.DataFrame(final_df)

    def ventures_cleansing(self):
        dataframes = []
        for i in range(10):
            file_path = (
                f'{os.path.abspath(self.ventures)}/Estabelecimentos{i}.csv'
            )
            df = read_csv_safely(file_path, self.ventures_columns, i)
            if df is not None:
                # print(f'Dataframe {i} lido com sucesso. Shape {df.shape}')
                dataframes.append(df)
            else:
                print(f'Dataframe {i} não pode ser lido')

        new_df = self._concatenate_dataframes(dataframes)

        new_df = new_df.filter(pl.col('situacao_cadastral') == 2)

        new_df = new_df.with_columns(
            pl.col('identificador_matriz_filial').replace_strict(
                [1, 2], ['Matriz', 'Filial']
            ),
            pl.col('situacao_cadastral').replace_strict([2], ['Ativa']),
            pl.col('data_situacao_cadastral')
            .cast(pl.String)
            .str.to_date('%Y%m%d', strict=False),
            pl.col('data_inicio_atividade')
            .cast(pl.String)
            .str.to_date('%Y%m%d', strict=False),
            pl.col('data_situacao_especial')
            .cast(pl.String)
            .str.to_date('%Y%m%d', strict=False),
            pl.col('cnae_secundario')
            .str.split(',')
            .list.first()
            .cast(pl.Int64, strict=False),
        )
        print(f'ventures: {new_df.dtypes}')
        return pl.DataFrame(new_df)

    def municipalities_cleansing(self):
        try:
            df = read_csv_safely(
                f'{os.path.abspath(self.municipalities)}/Municipios.csv',
                self.municipalities_columns,
            )
            if df is not None:
                # print(df.head(3))
                return pl.DataFrame(df)
        except Exception as e:
            print(f'Não foi possível ler o Dataframe: {e}')

    def countries_cleansing(self):
        try:
            df = read_csv_safely(
                f'{os.path.abspath(self.countries)}/Paises.csv',
                self.countries_columns,
            )

            if df is not None:
                df = df.with_columns(pl.col('codigo').cast(pl.String))
                print(df.dtypes)
                return df
        except Exception as e:
            print(f'Não possível ler o Dataframe: {e}')

    def cnae_description(self):

        try:
            df = read_csv_safely(
                f'{os.path.abspath(self.cnaes)}/Cnaes.csv',
                self.code_description,
            )

            if df is not None:
                print(df.dtypes)
                return df
        except Exception as e:
            print(f'Não foi possível ler o Dataframe: {e}')

    def join_dataframes(self):

        companies = self.companies_cleansing()
        ventures = self.ventures_cleansing()
        muncipalities = self.municipalities_cleansing()
        countries = self.countries_cleansing()
        cnaes = self.cnae_description()

        try:
            first_join = companies.join(
                ventures, on='cnpj_basico', how='inner'
            )
            second_join = first_join.join(
                muncipalities,
                left_on='municipio',
                right_on='codigo',
                how='left',
            )
            third_join = second_join.join(
                countries, left_on='pais', right_on='codigo', how='left'
            )
            fourth_join = third_join.join(
                cnaes, left_on='cnae_principal', right_on='codigo', how='left'
            ).rename({'descricao': 'descricao_cnae_principal'})
            final_df = fourth_join.join(
                cnaes, left_on='cnae_secundario', right_on='codigo', how='left'
            ).rename({'descricao': 'descricao_cnae_secundario'})

            if final_df is not None:
                final_df = final_df.with_columns(
                    pl.concat_str(
                        [
                            final_df['cnpj_basico'],
                            final_df['cnpj_ordem'],
                            final_df['cnpj_dv'],
                        ],
                        separator='',
                    ).alias('cnpj'),
                )

                del_columns = [
                    'cnpj_basico',
                    'cnpj_ordem',
                    'cnpj_dv',
                    'motivo_situacao_cadastral',
                    'fax',
                    'ddd_fax',
                    'municipio',
                ]

                final_df = final_df.drop(del_columns)
                sorted_df = ['cnpj'] + [
                    col for col in final_df.columns if col != 'cnpj'
                ]
                final_df = final_df.select(sorted_df)

                final_df.write_csv(
                    'result.csv',
                    separator=';',
                    include_header=True,
                    include_bom=True,
                )

            return pl.DataFrame(final_df)

        except Exception as e:
            print(f'Algo deu errado nos joins: {e}')
