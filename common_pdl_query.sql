with product_info
as
  (
   select distinct production_date
     from fdb.nddf_product_info
    order by 1 desc
    offset 1 rows fetch next 1 rows only
   )
select formulary_name, change_mode, drug_category,  rx_otc_medid,  tier_discrepancy_flg,  change_descr, change_descr_comments, start_dt, end_dt,  "FDB DATE ADDED", ndc,  medid,  gpi,  gpi_2_desc,  gpi_4_desc,  gpi_6_desc,  etc_category_class, 
       mony_code,  drug_name,  med_strength,  med_strength_uom,  gcrt_desc,  med_dosage_form_desc,  cl,  "CL DESC",  gni,  gni_desc,  nda_anda,  repack,  repack_desc,  ndcgi1,  source, 
       "Current Formulary Observations",  formulary_flg,  pdl_fm_tier,old_drug_tier_cd,  "Payer Decision",  ql,  "Daily/Over Time/Per Rx",  "Days Supply Limitation",  
        substr(pa, (instr(pa, '|', 1, 1) + 1)) pa,
        replace(trim(substr(pa, 1, (instr(pa, '|', 1, 1) - 1))), ' ', '/') "APA/EPA Flag",
        st,  gender,  age, 
        "Specialty Network Restriction",  "Prescriber Restriction",  "Brand Penalty Exemption",  "Comments"
 from(
     select formulary_name,change_mode,
            case when z.med_name is null then case when generic_medid>0 then  'New Brand' else 'New Generic' end
                 when y.medid is null then  case when generic_medid>0 then  'Line Ext Brand' else 'Line Ext Generic' end
                 else 'New NDC of Existng drugs'
            end drug_category,
            ( select  distinct  case when count(distinct a.cl) over(partition by medid)=2 then 'Y' else 'N' end
                from  fdb.rndc14_ndc_mstr a,fdb.rmindc1_ndc_medid b
               where a.ndc=b.ndc
                 and b.medid=x.medid
            )rx_otc_medid,
           ( select case when count(distinct fd.drug_tier_id_rx )>1 then 'Y' else 'N' end
              from claim.formulary_detl fd, claim.drug_tier dt
            where fd.drug_tier_id_rx=dt.drug_tier_id_rx
               and trunc(sysdate) between frml_detl_start_dt and frml_detl_end_dt
               and nvl(fd.delete_flg,'N')='N' and nvl(dt.delete_flg,'N')='N'
               and fd.medid=x.medid
               and formulary_id_rx=x.formulary_id_rx
           )tier_discrepancy_flg,
           x.change_descr,change_descr_comments,x.start_dt, x.end_dt,x."FDB DATE ADDED",x.ndc,x.medid,coalesce (x.gpi,gpi_medid)gpi,
           (select tcgpi_name from mds.mf2tcgpi where tcgpi_id=substr(coalesce (x.gpi,gpi_medid),1,2)and nvl(transaction_code,'A')<>'D' and rownum=1)gpi_2_desc,
           (select tcgpi_name from mds.mf2tcgpi where tcgpi_id=substr(coalesce (x.gpi,gpi_medid),1,4)and nvl(transaction_code,'A')<>'D' and rownum=1)gpi_4_desc,
           (select tcgpi_name from mds.mf2tcgpi where tcgpi_id=substr(coalesce (x.gpi,gpi_medid),1,6)and nvl(transaction_code,'A')<>'D' and rownum=1)gpi_6_desc,
           (select etc_name from fdb.mw_etc_ndc where  ndc=x.ndc)etc_category_class,
           (select multi_source_code from mds.mf2ndc where ndc_upc_hri=x.ndc)mony_code,
           x.drug_name,x.med_strength,x.med_strength_uom,x.gcrt_desc,x.med_dosage_form_desc,x.cl,x."CL DESC",x.gni,x.gni_desc,x.nda_anda,x.repack,x.repack_desc,x.ndcgi1,x.source,x."Current Formulary Observations",x.formulary_flg,x.pdl_fm_tier,x.old_drug_tier_cd,x."Payer Decision",
( select qlg.ql_group_id_rx || ' - ' || ql_group_descr
               from claim.quantity_limit_group qlg, claim.quantity_limit ql, claim.ql_frm_link lnk
              where qlg.ql_group_id_rx = ql.ql_group_id_rx
                and ql.medid = x.medid
                and lnk.ql_id_rx = ql.ql_id_rx
                and lnk.ql_id_rx = nvl(x.ql_id_rx, ql.ql_id_rx)
                and least(x.end_dt, greatest(x.start_dt, trunc(sysdate))) between trunc(lnk.start_dt) and trunc(lnk.end_dt)
                and lnk.formulary_id_rx = x.formulary_id_rx
                and nvl(qlg.delete_flg,'N')='N'
                and nvl(ql.delete_flg,'N')='N'
                and nvl(lnk.delete_flg,'N')='N'
                and rownum=1
           ) "QL",null "Daily/Over Time/Per Rx",null "Days Supply Limitation",
           ( select case when nvl(pdl.apa_auto_flg, 'N') = 'Y' and not exists (select 1 from claim.formulary_cfg fc 
                                                                               where fc.formulary_id_rx = lnk.formulary_id_rx
                                                                               and fc.drug_tier_cd = x.pdl_fm_tier
                                                                               and fc.cfg_type = 36603 and fc.cfg_value = 'N'
                                                                              )
                         then 'APA ' end  ||
                    case when nvl(pdl.epa_auto_flg, 'N') = 'Y' and not exists (select 1 from claim.formulary_cfg fc 
                                                                               where fc.formulary_id_rx = lnk.formulary_id_rx
                                                                               and fc.drug_tier_cd = x.pdl_fm_tier
                                                                               and fc.cfg_type = 36602 and fc.cfg_value = 'N'
                                                                              )
                         then 'EPA' end
                    || '|' ||
                    pa_group_id || ' - ' || pa_group_descr
               from claim.pa_group_rx pa, claim.pa_drug_list pdl, claim.pa_drug_list_frm_link lnk
              where pa.pa_group_id_rx = pdl.pa_group_id_rx
                and pdl.medid = x.medid
                and lnk.pa_drug_list_id_rx = pdl.pa_drug_list_id_rx
                and lnk.pa_drug_list_id_rx = nvl(x.pa_drug_list_id_rx, pdl.pa_drug_list_id_rx)
                and least(x.end_dt, greatest(x.start_dt, trunc(sysdate))) between trunc(lnk.start_dt) and trunc(lnk.end_dt)
                and lnk.formulary_id_rx = x.formulary_id_rx
                and nvl(pa.delete_flg,'N')='N'
                and nvl(pdl.delete_flg,'N')='N'
                and nvl(lnk.delete_flg,'N')='N'
                and not exists (select 1 from claim.formulary_cfg fc 
                                where formulary_id_rx = lnk.formulary_id_rx
                                and cfg_type = 36601 
                                and to_char(pdl.pa_group_id_rx) = fc.cfg_value
                                and fc.drug_tier_cd != x.pdl_fm_tier
                               ) 
                and not exists (select 1 from claim.pa_drug_list_exc pde
                                where pde.pa_drug_list_id_rx = pdl.pa_drug_list_id_rx
                                and pde.exc_id_qlf = 28914 --NDC
                                and pde.exc_id = x.ndc
                                and nvl(pde.delete_flg, 'N') = 'N'
                               ) 
                and rownum = 1
           ) pa,
           
           
           
                      ( select STEP_THERAPY_GROUP_ID || ' - ' || STEP_THERAPY_GROUP_NAME
               from claim.step_therapy_group stg, claim.step_therapy st, claim.step_therapy_frm_link lnk
              where stg.STEP_THERAPY_GROUP_ID_RX = st.STEP_THERAPY_GROUP_ID_RX
                and st.medid = x.medid
                and lnk.step_therapy_id_rx = st.step_therapy_id_rx
                and lnk.step_therapy_id_rx = nvl(x.step_therapy_id_rx, st.step_therapy_id_rx)
                and least(x.end_dt, greatest(x.start_dt, trunc(sysdate))) between trunc(lnk.start_dt) and trunc(lnk.end_dt)
                and lnk.formulary_id_rx = x.formulary_id_rx
                and nvl(stg.delete_flg,'N')='N'
                and nvl(st.delete_flg,'N')='N'
                and nvl(lnk.delete_flg,'N')='N'
                and rownum=1
           ) st,null gender,null age,null "Specialty Network Restriction",null "Prescriber Restriction",null "Brand Penalty Exemption",null "Comments"
    from (
            select formulary_name,decode(change_mode,'FRML', 'Formulary Detail','CHG_MGMT',' Change Management')change_mode,
                   decode(change_type,1900,'addition', 1901, 'Deletion', 1902,'Updation')change_descr,
                   decode(status, 'UPD_END_DT','Date Update','UPD_WRAP','Wrap to State Change')change_descr_comments,
                   decode(change_type,1901, null, start_dt)start_dt,
                   end_dt,
                   change_eff_dt,
                   pa_drug_list_id_rx,
                   ql_id_rx,
                   step_therapy_id_rx,
                   daddnc "FDB DATE ADDED",
                   rnm.ndc,
                   f.medid,
                   (select generic_product_identifier from mds.mf2name nm ,mds.mf2ndc ndc
                     where nm.drug_descriptor_id = ndc.drug_descriptor_id and ndc_upc_hri=rnm.ndc
                    )gpi,
                    (select generic_product_identifier
                     from claim.vw_gpi_medid  where medid=b.medid and rownum=1
                     )gpi_medid,
                    d.med_name drug_name,
                    b.med_strength,
                    b.med_strength_uom,
                    gcrt_desc,
                    c.med_dosage_form_desc,
                    cl ,
                    case when cl='F' then 'Rx' when cl='O' then 'OTC' else null end  "CL DESC",
                    gni,
                    case when gni='2'    then 'BRAND' when gni='1' then'GENERIC' else 'OTHER' end gni_desc,
                    case when nda_ind=1  and  anda_ind=1 then 'NDA/ANDA'
                         when nda_ind=1  then 'NDA'
                         when anda_ind=1 then 'ANDA' else null
                     end nda_anda,
                    repack,
                    case when repack=1 then 'Repackager'else 'Not Repackager' end repack_desc,
                    ndcgi1,
                    case when ndcgi1='1' then 'MULTI' when  ndcgi1='2' then 'SINGLE' end source,
                    null "Current Formulary Observations",
                    ( select 'Y' from claim.formulary_detl fd , claim.drug_tier dt where  fd.drug_tier_id_rx=dt.drug_tier_id_rx and fd.medid=f.medid
                     and trunc(sysdate) between frml_detl_start_dt and frml_detl_end_dt   and dt.formulary_id_rx =rxa_detl.formulary_id_rx
                     and nvl(dt.delete_flg,'N')='N'and nvl(fd.delete_flg,'N')='N' and rownum=1
                     )formulary_flg,
                    drug_tier_cd pdl_fm_tier,
                    old_drug_tier_cd,
                    rxa_detl.formulary_id_rx,
                    null "Payer Decision",rxa_detl.pa_group_id_rx
             from claim.pdl_frml_detl rxa_detl,
                  fdb.rndc14_ndc_mstr rnm,
                  fdb.rmidfid1_routed_dose_form_med a,
                  fdb.rmiid1_med b,
                  fdb.rmidfd1_dose_form  c,
                  fdb.rminmid1_med_name d,
                  fdb.rmirmid1_routed_med e,
                  fdb.rmindc1_ndc_medid f,
                  fdb.rgcnseq4_gcnseqno_mstr gcnseq,
                  fdb.rrouted3_route_desc gc ,
                  fdb.rapplsl0_fda_ndc_nda_anda fda,
                  claim.formulary fm
            where 1 = 1 --rxa_detl.formulary_id_rx = 5875
              and a.routed_dosage_form_med_id   = b.routed_dosage_form_med_id
              and a.med_dosage_form_id          = c.med_dosage_form_id
              and a.routed_med_id               = e.routed_med_id
              and d.med_name_id                 = e.med_name_id
              and b.medid                       = f.medid
              and rnm.ndc                       = f.ndc
              and gcnseq.gcn_seqno              = rnm.gcn_seqno
              and gcnseq.gcrt                   = gc.gcrt
              and rnm.ndc                       = fda.ndc(+)
              and rnm.ndc                       = rxa_detl.ndc
              and rxa_detl.formulary_id_rx=fm.formulary_id_rx
              and status                        <> 'ERROR'
            )x,
            fdb.rmiid1_med w,
            ( select distinct  a.medid
                from fdb.rmindc1_ndc_medid a ,fdb.rndc14_ndc_mstr b ,product_info
               where a.ndc=b.ndc
                 and trunc(b.daddnc)<=production_date
            )y ,
 (
              select  distinct  med_name
                from fdb.mw_etc_ndc a ,fdb.rndc14_ndc_mstr b ,product_info
               where a.ndc=b.ndc
                 and trunc(b.daddnc)<=production_date
            )z
      where x.medid=y.medid(+)
        and x.drug_name=z.med_name(+)
        and x.medid=w.medid
 )
