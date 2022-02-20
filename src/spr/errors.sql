create function spr.errors (
    p spr_.logger,
    n bigint,
    ph float,
    debit float,
    cod float,
    nh3n float,
    tss float,
    load_cod float default null,
    load_nh3n float default null,
    load_tss float default null,
    load_total float default null)
returns text[] as $$
    select array_agg(err) from (

        select case when p.has_ph and ph is null then 'error.missing_ph' else null end
        union
        select case when p.has_ph and ph<p.min_ph then 'error.below_min_ph' else null end
        union
        select case when p.has_ph and ph>p.max_ph then 'error.above_max_ph' else null end
        union

        select case when p.has_debit and debit is null then 'error.missing_debit' else null end
        union
        select case when p.has_debit and debit<0 then 'error.below_zero_debit' else null end
        union
        select case when p.has_debit and debit>p.max_debit then 'error.above_max_debit' else null end
        union

        select case when p.has_cod and cod is null then 'error.missing_cod' else null end
        union
        select case when p.has_cod and cod<0 then 'error.below_zero_cod' else null end
        union
        select case when p.has_cod and cod>p.max_cod then 'error.above_max_cod' else null end
        union

        select case when p.has_nh3n and nh3n is null then 'error.missing_nh3n' else null end
        union
        select case when p.has_nh3n and nh3n<0 then 'error.below_zero_nh3n' else null end
        union
        select case when p.has_nh3n and nh3n>p.max_nh3n then 'error.above_max_nh3n' else null end
        union

        select case when p.has_tss and tss is null then 'error.missing_tss' else null end
        union
        select case when p.has_tss and tss<0 then 'error.below_zero_tss' else null end
        union
        select case when p.has_tss and tss>p.max_tss then 'error.above_max_tss' else null end
    ) errs (err)
    where err is not null
$$ language sql stable;
