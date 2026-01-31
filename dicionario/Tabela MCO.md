# Dicionário de Dados - MCO (Mapa de Controle Operacional)

| Atributo | Tipo | Descrição |
|----------|------|-----------|
| VIAGEM | Alfanumérico | Data em que a viagem foi realizada; ex.: 29/07/2018. |
| LINHA | Alfanumérico | Número da linha em que a viagem foi realizada; ex.: SC01A. |
| SUBLINHA | Numérico | Número da sublinha em que a viagem foi realizada. |
| PC | Numérico | Número do Ponto de Controle (PC) de origem em que a viagem foi iniciada. Para viagens normais, podem ser PC 1 ou 2. Para viagens ociosas, PC 0, e viagens de transferência, PC 3. |
| CONCESSIONÁRIA | Numérico | Número da concessionária ao qual a linha está vinculada; pode ser 801 (Consórcio Pampulha), 802 (Consórcio BHLeste), 803 (Consórcio Dez) e 804 (Consórcio Dom Pedro II). |
| SAÍDA | Alfanumérico | Hora de saída da viagem do PC de origem. |
| VEÍCULO | Numérico | Número de ordem do veículo, cadastrado na BHTRANS, em que a viagem foi operada; ex.: 11484. |
| CHEGADA | Alfanumérico | Hora de chegada da viagem do PC de destino. |
| CATRACA SAIDA | Numérico | Catraca registrada no início da viagem com 5 dígitos; ex.: 45267. |
| CATRACA CHEGADA | Numérico | Catraca registrada no fim da viagem com 5 dígitos; ex.: 67787. |
| OCORRÊNCIA | Alfanumérico | Indicador se houve interrupção da viagem registrada no MCO conforme ANEXO 03 |
| JUSTIFICATIVA | Alfanumérico | Indicador do tipo de justificativa da ocorrência pela qual a viagem foi interrompida conforme ANEXO 02 |
| TIPO DIA | Numérico | Tipo do dia em que a viagem foi realizada conforme ANEXO 01 |
| EXTENSÃO | Numérico | Extensão da viagem realizada, em metros. |
| FALHA MECÂNICA | Alfanumérico | Indicador se houve falha mecânica durante a viagem; S para SIM e N para NÃO. |
| EVENTO INSEGURO | Alfanumérico | Indicador se houve evento inseguro durante a viagem; S para SIM e N para NÃO. |
| INDICADOR FECHAMENTO | Alfanumérico | Indicador se a viagem foi fechada; F para FECHADA |
| DATA FECHAMENTO | Alfanumérico | Data e hora do fechamento da viagem, p.ex.: 03/01/2020 15:44:00 |
| TOTAL USUÁRIOS | Alfanumérico | Número total de usuários da viagem registrados pelo sistema de bilhetagem eletrônica |
| EMPRESA | Alfanumérico | Código da empresa operadora do veículo que realizou a viagem |