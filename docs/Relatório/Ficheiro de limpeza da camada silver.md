# Camada Silver — Limpeza e Normalização

> Conteúdo de apoio para a Secção "Camada Silver: Transformação e Dicionário de Dados" do relatório (`main.tex`).

- **Nomes de colunas:** passam a minúsculas, sem espaços nem caracteres especiais (por exemplo `Day Mins` → `day_mins`, `downlink(mbps)` → `downlink_mbps`), de forma a serem estáveis em SQL e nos pipelines.
- **Números que vinham como texto:** removem-se unidades e símbolos (`dBm`, `Mbps`, `km/h`) e converte-se para valores decimais; vírgulas europeias (ex.: `2,1`) passam a ponto decimal.
- **Datas e horas:** unificam-se formatos distintos do CSV (com ou sem frações de segundo) num campo de data/hora único por tabela (`timestamp_log`, `date_of_test`).
- **Texto codificado em colunas sim/não:** o tipo de rede deixa de ser uma frase livre (*LTE*, *NR*, …) e passa a colunas do tipo **Sim/Não** — por exemplo `nt_ohe_lte` na telemetria, `tech_ohe_lte` nos testes e `radio_ohe_lte` nas torres — o que facilita filtros e agregações na camada *Gold*.
- **Resultados e estados booleanos:** o *churn* e o sucesso de um teste de chamada (`result`) ficam explícitos como Sim/Não; o estado da torre (`status`) indica se está activa ou indisponível.
- **Identificador por linha:** cada registo recebe `silver_row_id`, um número interno que não depende do ficheiro de origem.
- **Ligação entre tabelas:** o `phone_number` é normalizado como chave comum entre faturação, telemetria e testes, permitindo cruzar o mesmo cliente nas três fontes.

Registos que não resistem a estas regras (por exemplo, um número irreconhecível após limpeza) não entram na tabela principal: ficam em **quarentena**, com motivo do erro, para auditoria.

A seguir detalham-se, por conjunto de dados, o propósito da tabela, os problemas concretos encontrados na *Bronze* e as transformações correspondentes na *Silver*.

---

## Faturação e Retenção de Clientes (`cdr_customers`)

**Propósito:** Perfil comercial de cada cliente — consumo, faturação e se cancelou o contrato (*churn*).
Dicionário completo no Anexo `anexo:dic_cdr`.

**Estado na Bronze:** 101 174 registos, com um grave problema de duplicação.

**Problemas identificados:**

- 40 729 linhas exactamente duplicadas.

**Transformações Silver:**

- **Deduplicação:** remoção de todas as linhas duplicadas, garantindo que cada cliente (identificado por `phone_number`) possui apenas os registos únicos das suas chamadas e da sua *label* de *churn*.

---

## Telemetria de Rádio (`network_logs`)

**Propósito:** registos passivos do que o telemóvel mediu na rede (sinal, velocidade, posição em Leiria).
Dicionário completo no Anexo `anexo:dic_logs`.

**Estado na Bronze:** 10 570 registos com tipos de dados corrompidos por texto e valores geográficos ausentes.

**Problemas identificados:**

- Métricas numéricas gravadas como *strings* com unidades (ex.: `-99 dBm`, `29 Mbps`).
- 269 registos sem coordenadas (latitude/longitude).
- Colunas com espaços no nome (ex.: `Network provi. `).

**Transformações Silver:**

- **Parsing e casting:** remoção das *substrings* ` dBm`, ` dB`, ` Mbps`, ` km/h` e conversão das colunas para o tipo numérico (`Float`).
- **Data cleansing:** remoção (*drop*) das linhas onde as coordenadas geográficas são nulas.
- **Schema standardization:** limpeza (*trim*) dos nomes das colunas.
- **Translação geográfica:** aplicação de um *offset* matemático (+21.63 lat, −92.20 lon) para transladar os pontos originais da Índia para o centro do nosso cenário de negócio: Leiria, Portugal.

---

## Testes de Qualidade (`call_tests`)

**Propósito:** testes activos de chamada — se a ligação falhou, qualidade de voz (MOS) e distância à torre.
Dicionário completo no Anexo `anexo:dic_call_tests`.

**Estado na Bronze:** 105 828 registos.

**Problemas identificados:**

- Formatação europeia de decimais (uso de vírgulas `,` em vez de pontos `.`), o que força os motores analíticos a ler números como texto (ex.: MOS = `2,1`).
- 10 359 valores nulos na coluna `Distance from site (m)`.

**Transformações Silver:**

- **Casting de decimais:** substituição de `,` por `.` nas colunas métricas e conversão para o tipo `Float`.
- **Tratamento de nulos:** manutenção inicial dos nulos com *flag* para futura imputação (ou cruzamento espacial na camada *Gold*).

---

## Inventário de Torres (`towers`)

**Propósito:** localização e estado das antenas na região de Leiria (fonte OpenCelliD).
Dicionário completo no Anexo `anexo:dic_towers`.

**Estado na Bronze:** ficheiro limpo (28 360 linhas), mas geograficamente demasiado abrangente (todo o país).

**Transformações Silver:**

- **Filtro espacial (*bounding box*):** redução do dataset apenas às torres contidas no polígono da região de Leiria (lat: 39.5 a 39.9, lon: −9.0 a −8.6), optimizando o processamento computacional nos *JOINs* futuros.

---

## Integração Relacional e Simulação do Evento (Tempestade)

Como os ficheiros provêm de fontes independentes, a fase final da transformação *Silver* visa uni-los num modelo relacional coeso e introduzir o cenário hipotético que será alvo de análise:

- **Geração de chaves (*mocking*):** mapeamento aleatório mas determinístico dos `DeviceID` (dos *logs*) a `phone_number` (do CDR) e injecção destes números nos testes de chamada, criando uma chave estrangeira que liga a física da rede ao cliente faturado.
- **Injecção de anomalias (o "desastre"):** simulação da tempestade na zona Este de Leiria (longitude > −8.80) através do agravamento matemático do sinal (RSRP − 25) e injecção propositada de valores nulos (`NaN`) no ruído (SINR), simulando postes derrubados e falhas de comunicação.
