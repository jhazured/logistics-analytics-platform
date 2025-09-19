-- Fails if any shipment exceeds vehicle capacity
select s.shipment_id
from {{ ref(fact_shipments) }} s
join {{ ref(dim_vehicle) }} v on s.vehicle_id = v.vehicle_id
where s.volume_m3 > v.capacity_m3 or s.weight_kg > v.capacity_kg
