-- fact_shipments relationships
select shipment_id from {{ ref('fact_shipments') }} s
left join {{ ref('dim_customer') }} c on s.customer_id = c.customer_id
where c.customer_id is null
union all
select shipment_id from {{ ref('fact_shipments') }} s
left join {{ ref('dim_route') }} r on s.route_id = r.route_id
where r.route_id is null
union all
select shipment_id from {{ ref('fact_shipments') }} s
left join {{ ref('dim_date') }} d on to_date(s.shipment_date) = d.date
where d.date is null

