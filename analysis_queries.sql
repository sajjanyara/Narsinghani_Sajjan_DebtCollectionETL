-- 1
select AVG(loan_amount) as average_loan_amount
from public.borrowers
where days_left_to_pay_current_emi > 5 and delayed_payment = 'Yes';

-- 2
select Name,
       (loan_amount - (emi * loan_term)) as outstanding_balance
from public.borrowers
order by outstanding_balance desc
limit 10;

-- 3
select *
from public.borrowers
where Delayed_Payment = 'No';

-- 4
select loan_purpose,
       AVG(loan_amount) as average_loan_amount,
       SUM(loan_amount) as total_loan_amount,
       COUNT(*) as loan_count,
       AVG(interest_rate) as average_interest_rate
from public.borrowers
group by loan_purpose;
