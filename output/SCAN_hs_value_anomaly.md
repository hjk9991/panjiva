# HS 오분류로 금액이 부풀려진 사례 — 전수 탐색

**탐색일** 2026-08-28 · **스크립트** `scan_hs_value_anomaly.py` · **대상** 2024년 미국 수입 전수

`valueOfGoodsUSD` 는 **`HS별 단가 × 중량`으로 추정된 값**이므로 HS 가 틀리면 금액도 함께 틀어진다. 실측 근거: Mosaic 건의 kg 당 단가($7.24)가 HS 9503 전체 중위값($7.19)과 **1.01배** 일치.

**전수 18,134,776건 · $1,910.7B** 중 금액·중량이 다 있는 17,946,347건($1,910.7B)을 본다.


## 방법 A — 벌크 화물인데 단가가 비현실적으로 높다

기준: 중량 **10천 톤 초과** 이면서 kg 당 단가 **$5 초과**.

벌크선으로 오는 원자재(광석·곡물·비료·연료)는 kg 당 $0.05~1 수준이다. 그보다 훨씬 비싼 품목이 벌크로 오는 일은 드물다.

- 1만 톤 초과 선적: **8,612건 · $388.2B**
- 그 중위 단가: **$0.646/kg**

**의심 358건 · $123.95B**

| hs6 | 품목 | 수입자 | 수출자 | 원산지 | 건수 | 중량(천톤) | 금액($B) | 단가($/kg) |
|---|---|---|---|---|---|---|---|---|
| 950300 | 완구·게임 | Mosaic Fertilizer Llc | Compania Minera Miski Mayo Srl . | Peru | 23 | 1,182.12 | 8.65 | 7.24 |
| 381590 | 각종 화학공업 생산품 | Eastern Salt Co. Inc. | Compania Minera Cordillera Chile | Chile | 11 | 466.21 | 6.03 | 13.34 |
| 847982 | 기계 | Carver Sand & Gravel Se Llc | Carver Materials Canada Ltd. | Canada | 7 | 311.52 | 4.05 | 13.01 |
| 842890 | 기계 | Cemex Construction Materials | Cemex Materials Newfoundland Inc. | Canada | 13 | 376.04 | 3.79 | 10.36 |
| 842890 | 기계 | Carver Sand & Gravel Se Llc | Carver Materials Canada Ltd. | Canada | 7 | 305.52 | 3.35 | 11.25 |
| 761699 | 알루미늄 | Nucor Steel Louisiana Llc | Samarco Mineracao SA | Brazil | 5 | 387.13 | 2.67 | 7.22 |
| 761699 | 알루미늄 |  | Samarco Mineracao SA | Brazil | 5 | 388.89 | 2.54 | 6.49 |
| 382450 | 각종 화학공업 생산품 | Hollingshead Cement Llc | Lafarge Emirates Cement Llc | United Arab Emirates | 4 | 179.09 | 2.50 | 13.94 |
| 300490 | 의료용품 | Oxbow Calcining Llc | Oxbow Brasil Energia Industria E Comercio Ltda | Brazil | 4 | 51.04 | 2.35 | 46.50 |
| 382219 | 각종 화학공업 생산품 |  |  | Trinidad and Tobago | 4 | 49.36 | 2.32 | 47.48 |
| 870323 | 자동차 | Surface Deployment And Distribution | Surface Deployment And Distribution | United States | 8 | 154.49 | 2.27 | 14.36 |
| 300490 | 의료용품 | Oxbow Calcining Llc | Oxbow Energy Solutions B.V. | Netherlands | 3 | 48.90 | 2.27 | 49.00 |
| 845590 | 기계 |  | Vale SA | Brazil | 3 | 229.36 | 2.19 | 8.74 |
| 847982 | 기계 | Levy Docks Brennan Ave | Ontario Trap Rock | Canada | 7 | 124.00 | 2.15 | 18.54 |
| 382219 | 각종 화학공업 생산품 | Valenz Corp. | Methanol Holdings (Trinidad) Ltd. | Trinidad and Tobago | 3 | 41.15 | 1.98 | 51.02 |
| 090111 |  | Blue Water Industries Llc | Shoreline Aggregates Inc. | Canada | 7 | 369.25 | 1.95 | 5.16 |
| 847989 | 기계 | Arcelormittal Texas Hbi Llc | Samarco Mineracao SA | Brazil | 2 | 216.45 | 1.79 | 8.26 |
| 843139 | 기계 | Nucor Steel Louisiana Llc | Vale SA | Brazil | 3 | 234.51 | 1.77 | 7.53 |
| 300490 | 의료용품 | Oxbow Calcining Llc | Petroleum Coke Industries Co. | Kuwait | 2 | 40.00 | 1.64 | 40.98 |
| 300490 | 의료용품 | Oxbow Calcining Llc | Oxbow Brasil Energia Industries E | Brazil | 2 | 29.97 | 1.47 | 49.00 |

## 방법 B — 같은 (수입자, 수출자) 쌍이 여러 HS 로 갈리고 단가가 크게 벌어진다

기준: 같은 쌍에서 **HS 가 2개 이상**이고 **최고/최저 중위단가 비 10배 이상**, 양쪽 다 벌크(10천 톤 초과 선적 포함).

같은 거래관계에서 같은 규모의 화물이 오는데 HS 만 다르고 단가가 수십 배 차이나면, **어느 한쪽이 잘못 분류된 것**이다.

**의심 쌍 88개**

| 수입자 | 수출자 | HS 수 | 최저단가 | 최고단가 | 배수 | 총액($B) |
|---|---|---|---|---|---|---|
|  |  | 90 | 0.006 | 39.320 | 6,556.445 | 43.544 |
| Eastern Salt Co. Inc. | Compania Minera Cordillera Chile | 6 | 0.027 | 13.335 | 484.986 | 10.537 |
| Mosaic Fertilizer Llc | Compania Minera Miski Mayo Srl . | 5 | 0.107 | 7.238 | 67.646 | 10.416 |
| Carver Sand & Gravel Se Llc | Carver Materials Canada Ltd. | 6 | 0.640 | 13.012 | 20.331 | 7.617 |
| Morton Salt, Inc. | Sociedad Punta De Lobos S.A. | 11 | 0.020 | 9.343 | 467.150 | 4.643 |
| Cemex Construction Materials | Cemex Materials Newfoundland Inc. | 5 | 0.011 | 10.365 | 901.476 | 3.923 |
| Hollingshead Cement Llc | Lafarge Emirates Cement Llc | 5 | 0.595 | 13.937 | 23.423 | 3.602 |
| Nlmk Pennsylvania Corp. | Arcelormittal Pecem S.A. | 4 | 0.610 | 8.407 | 13.793 | 2.352 |
|  | Vale SA | 2 | 0.298 | 8.738 | 29.322 | 2.307 |
| Hm Southeast Cement Llc | Akcansa Cimento San Tic. A.S. Tehc Trading Americas Llc | 5 | 0.595 | 13.937 | 23.423 | 2.050 |
| Nucor Steel Louisiana Llc | Vale SA | 2 | 0.331 | 7.527 | 22.740 | 1.839 |
| Valero Marketing And Supply Co. | Valero Energy Inc. | 7 | 0.594 | 15.766 | 26.542 | 1.838 |
| Heidelberg Marterials US Cement Llc | Akcansa Cimento Sanayi Ve Ticaret A | 3 | 0.792 | 9.438 | 11.916 | 1.804 |
| Sunoco Inc. | Hd Hyundai Oilbank Co., Ltd. | 3 | 0.952 | 13.328 | 14.000 | 1.781 |
| Valero Marketing And Supply Co. | Pmi Trading Designated Activity Co. | 3 | 0.551 | 19.566 | 35.510 | 1.656 |

### 상위 5개 쌍의 HS 내역


**nan  ←  nan**

| hs6 | 품목 | 건수 | 금액($B) | 중량(천톤) | 중위중량(천톤) | 단가($/kg) |
|---|---|---|---|---|---|---|

**Eastern Salt Co. Inc.  ←  Compania Minera Cordillera Chile**

| hs6 | 품목 | 건수 | 금액($B) | 중량(천톤) | 중위중량(천톤) | 단가($/kg) |
|---|---|---|---|---|---|---|
| 381590 | 각종 화학공업 생산품 | 12 | 6.532 | 503.512 | 45.684 | 13.335 |
| 382000 | 각종 화학공업 생산품 | 10 | 1.964 | 459.367 | 50.727 | 4.278 |
| 381190 | 각종 화학공업 생산품 | 11 | 1.698 | 513.282 | 51.300 | 3.308 |
| 381121 | 각종 화학공업 생산품 | 1 | 0.208 | 51.960 | 51.960 | 3.996 |
| 382319 | 각종 화학공업 생산품 | 2 | 0.130 | 109.900 | 54.950 | 1.186 |
| 250100 | 소금·황·토석 | 6 | 0.005 | 196.001 | 40.104 | 0.027 |

**Mosaic Fertilizer Llc  ←  Compania Minera Miski Mayo Srl .**

| hs6 | 품목 | 건수 | 금액($B) | 중량(천톤) | 중위중량(천톤) | 단가($/kg) |
|---|---|---|---|---|---|---|
| 950300 | 완구·게임 | 23 | 8.654 | 1,182.120 | 51.401 | 7.238 |
| 283650 | 무기화학 | 7 | 1.062 | 409.949 | 59.893 | 2.377 |
| 230990 | 사료 | 7 | 0.517 | 321.803 | 50.461 | 1.662 |
| 251020 | 소금·황·토석 | 22 | 0.142 | 1,371.211 | 59.100 | 0.107 |
| 440711 | 목재 | 1 | 0.041 | 48.920 | 48.920 | 0.828 |

**Carver Sand & Gravel Se Llc  ←  Carver Materials Canada Ltd.**

| hs6 | 품목 | 건수 | 금액($B) | 중량(천톤) | 중위중량(천톤) | 단가($/kg) |
|---|---|---|---|---|---|---|
| 847982 | 기계 | 7 | 4.053 | 311.518 | 44.690 | 13.012 |
| 842890 | 기계 | 7 | 3.353 | 305.522 | 43.823 | 11.251 |
| 940161 | 가구 | 1 | 0.073 | 20.288 | 20.288 | 3.602 |
| 680223 | 석·시멘트 제품 | 2 | 0.055 | 76.730 | 38.365 | 0.721 |
| 681019 | 석·시멘트 제품 | 1 | 0.044 | 36.507 | 36.507 | 1.196 |
| 680293 | 석·시멘트 제품 | 2 | 0.039 | 61.572 | 30.786 | 0.640 |

**Morton Salt, Inc.  ←  Sociedad Punta De Lobos S.A.**

| hs6 | 품목 | 건수 | 금액($B) | 중량(천톤) | 중위중량(천톤) | 단가($/kg) |
|---|---|---|---|---|---|---|
| 382000 | 각종 화학공업 생산품 | 16 | 2.626 | 642.097 | 48.475 | 3.915 |
| 280469 | 무기화학 | 8 | 0.779 | 255.092 | 30.800 | 3.052 |
| 382499 | 각종 화학공업 생산품 | 1 | 0.464 | 49.667 | 49.667 | 9.343 |
| 382319 | 각종 화학공업 생산품 | 5 | 0.223 | 192.573 | 39.000 | 1.142 |
| 381121 | 각종 화학공업 생산품 | 1 | 0.152 | 38.130 | 38.130 | 3.996 |
| 381190 | 각종 화학공업 생산품 | 1 | 0.113 | 34.100 | 34.100 | 3.308 |
| 291590 | 유기화학 | 1 | 0.093 | 30.800 | 30.800 | 3.034 |
| 291570 | 유기화학 | 1 | 0.075 | 35.000 | 35.000 | 2.134 |
| 261310 | 광석·슬래그 | 6 | 0.060 | 225.587 | 33.193 | 0.256 |
| 290531 | 유기화학 | 2 | 0.055 | 60.358 | 30.179 | 0.902 |
| 250100 | 소금·황·토석 | 1 | 0.001 | 40.300 | 40.300 | 0.020 |

---

## 금액 영향 추정

의심 선적의 금액을 **같은 쌍의 최저 단가**(= 실제 화물로 추정되는 쪽)로 다시 계산하면:

| 수입자 | 수출자 | 현재 금액($B) | 재계산($B) | 과대 추정($B) | 배수 |
|---|---|---|---|---|---|
| Eastern Salt Co. Inc. | Compania Minera Cordillera Chile | 10.53 | 0.05 | 10.49 | 233.84 |
| Mosaic Fertilizer Llc | Compania Minera Miski Mayo Srl . | 10.23 | 0.20 | 10.03 | 49.97 |
| Carver Sand & Gravel Se Llc | Carver Materials Canada Ltd. | 7.41 | 0.39 | 7.01 | 18.75 |
| Morton Salt, Inc. | Sociedad Punta De Lobos S.A. | 4.64 | 0.03 | 4.61 | 148.45 |
| Cemex Construction Materials | Cemex Materials Newfoundland Inc. | 3.92 | 0.01 | 3.92 | 674.98 |
| Hollingshead Cement Llc | Lafarge Emirates Cement Llc | 2.50 | 0.11 | 2.39 | 23.42 |
| Nucor Steel Louisiana Llc | Vale SA | 1.77 | 0.08 | 1.69 | 22.74 |
| Sunoco Inc. | Hd Hyundai Oilbank Co., Ltd. | 1.74 | 0.13 | 1.61 | 13.14 |
| Dunham Price Group Llc | Tajo Chirripo SA | 1.51 | 0.00 | 1.51 | 568.47 |
| Alcoa Carbon Products | Bp Energia Espana S.A.U | 1.46 | 0.01 | 1.45 | 235.29 |
| Heidelberg Marterials US Cement Llc | Akcansa Cimento Sanayi Ve Ticaret A | 1.41 | 0.12 | 1.29 | 12.07 |
| Hm Southeast Cement Llc | Akcansa Cimento San Tic. A.S. Tehc Trading Americas Llc | 1.34 | 0.07 | 1.27 | 20.45 |

- **의심 쌍 전체의 과대 추정 합계: 약 $68.7B**
- 2024년 수입 총액 $1,910.7B 의 **3.60%**
- within_firm 총액 $206.0B 대비: **33.35%** (그룹내 거래에 집중돼 있다면 영향이 더 크다)

> ⚠️ **이 표는 '의심 후보'이지 확정 오류가 아니다.** 같은 쌍이 실제로 다른 품목을 함께 거래할 수도 있다. **원본 `panjivaUSImpHSCode` 와 화물 정황(중량·수량·단위·선박)을 함께 보고 판정해야 한다.**

> 확정된 사례는 **Mosaic Fertilizer ← Compania Minera Miski Mayo** 한 건이다. 같은 광산이 같은 규모(5~8만 톤)의 화물을 보내는데 HS 950300(완구, $7.878/kg)과 HS 251020(인산칼슘, $0.097/kg)으로 갈려 있고, S&P 원본이 `Classified: 9503.00` 임을 Snowflake 에서 직접 확인했다.

---

의심 선적 전수 **358건**을 `hs_value_anomaly_candidates.csv` 로 저장했다 (02 검토·S&P 문의용).