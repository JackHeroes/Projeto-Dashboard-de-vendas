import psycopg2

def execute_query(query):
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="Anfeidrol2468"
    )

    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()

    for row in results:
        print(row)

    print()
    
    cursor.close()
    conn.close()

''' 
    (Query 1) Receita, leads, conversão e ticket médio mês a mês
    Colunas: mês, leads (#), vendas (#), receita (k, R$), conversão (%), ticket médio (k, R$)
'''
query1 = """
WITH 
    leads AS (
        SELECT
            date_trunc('month', visit_page_date)::date AS visit_page_month,
            COUNT(*) AS visit_page_count
        FROM sales.funnel
        GROUP BY visit_page_month
        ORDER BY visit_page_month
    ),
    payments AS (
        SELECT
            date_trunc('month', fun.paid_date)::date AS paid_month,
            COUNT(fun.paid_date) AS paid_count,
            SUM(pro.price * (1 + fun.discount)) AS receita
        FROM sales.funnel fun
        LEFT JOIN sales.products pro 
            ON fun.product_id = pro.product_id
        WHERE fun.paid_date IS NOT NULL
        GROUP BY paid_month
        ORDER BY paid_month
    )

SELECT
    leads.visit_page_month AS "Mês",
    leads.visit_page_count AS "Leads (#)",
    payments.paid_count AS "Vendas (#)",
    (payments.receita / 1000) AS "Receita (k, R$)",
    (payments.paid_count::float / leads.visit_page_count::float) AS "Conversão (k, R$)",
    (payments.receita / payments.paid_count / 1000) AS "Ticket médio (k, R$)"
FROM leads
LEFT JOIN payments 
    ON leads.visit_page_month = payments.paid_month
"""

'''
    (Query 2) Estados que mais venderam
    Colunas: país, estado, vendas (#)
'''
query2 = """
select 
	'brazil' as "País",
	cus.state as "Estado",
	count(*) as "Vendas (#)"
from sales.funnel fun
left join sales.customers cus
	on fun.customer_id = cus.customer_id
where paid_date between '2021-08-01' and '2021-08-31'
group by "Estado"
order by "Vendas (#)" desc
limit 5
"""

'''
    (Query 3) Marcas que mais venderam no mês
    Colunas: marca, vendas (#)
'''
query3 = """
select
	pro.brand as "Marca",
	count(*) as "Vendas (#)"
from sales.funnel fun
left join sales.products pro
	on fun.product_id = pro.product_id
where paid_date between '2021-08-01' and '2021-08-31'
group by "Marca"
order by "Vendas (#)" desc
limit 5
"""

'''
    (Query 4) Lojas que mais venderam
    Colunas: loja, vendas (#)
'''
query4 = """
select
	sto.store_name as "Loja",
	count(*) as "Vendas (#)"
from sales.funnel fun
left join sales.stores sto
	on fun.store_id = sto.store_id
where paid_date between '2021-08-01' and '2021-08-31'
group by "Loja"
order by "Vendas (#)" desc
limit 5
"""

'''
    (Query 5) Dias da semana com maior número de visitas ao site
    Colunas: dia_semana, dia da semana, visitas (#)
'''
query5 = """
select
	extract('dow' from visit_page_date) as  "Codigo do dia",
	case
		when extract ('dow' from visit_page_date) = 0 then 'domingo'
		when extract ('dow' from visit_page_date) = 1 then 'segunda'
		when extract ('dow' from visit_page_date) = 2 then 'terça'
		when extract ('dow' from visit_page_date) = 3 then 'quarta'
		when extract ('dow' from visit_page_date) = 4 then 'quinta'
		when extract ('dow' from visit_page_date) = 5 then 'sexta'
		when extract ('dow' from visit_page_date) = 6 then 'sabado'
		else null
	end as "Dia da semana",
	count(*) as "Visitas (#)"
from sales.funnel fun
where paid_date between '2021-08-01' and '2021-08-31'
group by "Codigo do dia"
order by "Codigo do dia"
"""

execute_query(query1)
execute_query(query2)
execute_query(query3)
execute_query(query4)
execute_query(query5)