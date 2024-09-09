#' @title Metadata
#'
#' @rdname SAIL-Metadata
#'
#' @description
#' A list of datasets, table references and other associated metadata to be used
#' as a \code{DatasetContainer} lookup & constants. Please use the \code{View()} function
#' to see the available options, _e.g._ \code{View(SAILDB.METADATA)}
#'
#' @details
#' Each of the datasets are defined such that:
#' \preformatted{
#' # The key, i.e. [some.name], is the reserved dataset name
#' some.name   = list(
#'   ref    = 'some.name',  # an internal reference to the reserved name
#'   name   = 'SOME_NAME',  # the name of the table which may or may not contain a prefix, e.g. 'WLGP_'
#'   origin = 'SAILXXXXV',  # a reference to the base table from which this table can be found
#'   tag    = '_${date}',   # how refresh dates are tagged, i.e. some interpolated suffix; if \code{FALSE | NA} then no interpolation is performed
#'   alt    = 'WDSD',       # a prefix to the table name when defined as a character string; if \code{FALSE | NA} then no prefix will be added
#'   static = FALSE         # the \code{static} property describes whether the table is held by a reference schema instead of being copied to a project schema
#' )
#' }
#'
#' @export
#'
SAILDB.METADATA = list(
  # ABDE
  abde.births = list(
    ref    = 'abde.births',
    name   = 'BIRTHS',
    origin = 'SAILABDEV',
    tag    = '_${date}',
    alt    = 'ABDE'
  ),


  # MIDS
  mids.births = list(
    ref    = 'mids.births',
    name   = 'BIRTHS',
    origin = 'SAILMIDSV',
    tag    = '_${date}',
    alt    = 'MIDS'
  ),
  mids.init.assessment = list(
    ref    = 'mids.init.assessment',
    name   = 'INITIAL_ASSESSMENT',
    origin = 'SAILMIDSV',
    tag    = '_${date}',
    alt    = 'MIDS'
  ),


  # ADDE
  adde.deaths = list(
    ref    = 'adde.deaths',
    name   = 'DEATHS',
    origin = 'SAILADDEV',
    tag    = '_${date}',
    alt    = 'ADDE'
  ),


  # WDSD
  wdsd.pers = list(
    ref    = 'wdsd.pers',
    name   = 'AR_PERS',
    origin = 'SAILWDSDV',
    tag    = '_${date}',
    alt    = 'WDSD'
  ),
  wdsd.pers.gp = list(
    ref    = 'wdsd.pers.gp',
    name   = 'AR_PERS_GP',
    origin = 'SAILWDSDV',
    tag    = '_${date}',
    alt    = 'WDSD'
  ),
  wdsd.pers.add = list(
    ref    = 'wdsd.pers.add',
    name   = 'AR_PERS_ADD',
    origin = 'SAILWDSDV',
    tag    = '_${date}',
    alt    = 'WDSD'
  ),
  wdsd.pers.prv = list(
    ref    = 'wdsd.pers.prv',
    name   = 'AR_PERS_PRV',
    origin = 'SAILWDSDV',
    tag    = '_${date}',
    alt    = 'WDSD'
  ),
  wdsd.pers.gp.prv = list(
    ref    = 'wdsd.pers.gp.prv',
    name   = 'AR_PERS_GP_PRV',
    origin = 'SAILWDSDV',
    tag    = '_${date}',
    alt    = 'WDSD'
  ),
  wdsd.pers.add.prv = list(
    ref    = 'wdsd.pers.add.prv',
    name   = 'AR_PERS_ADD_PRV',
    origin = 'SAILWDSDV',
    tag    = '_${date}',
    alt    = 'WDSD'
  ),
  wdsd.res.gpreg = list(
    ref    = 'wdsd.res.gpreg',
    name   = 'PER_RESIDENCE_GPREG',
    origin = 'SAILWDSDV',
    tag    = '_${date}',
    alt    = 'WDSD'
  ),
  wdsd.c.add.wales = list(
    ref    = 'wdsd.c.add.wales',
    name   = 'WDSD_CLEAN_ADD_WALES',
    origin = 'SAILX0675V',
    tag    = '_${date}',
    alt    = FALSE
  ),
  wdsd.c.add.geog.lsoa01 = list(
    ref    = 'wdsd.c.add.geog.lsoa01',
    name   = 'WDSD_CLEAN_ADD_GEOG_CHAR_LSOA2001',
    origin = 'SAILX0675V',
    tag    = '_${date}',
    alt    = FALSE
  ),
  wdsd.c.add.geog.lsoa11 = list(
    ref    = 'wdsd.c.add.geog.lsoa11',
    name   = 'WDSD_CLEAN_ADD_GEOG_CHAR_LSOA2011',
    origin = 'SAILX0675V',
    tag    = '_${date}',
    alt    = FALSE
  ),
  wdsd.c.add.lsoa01 = list(
    ref    = 'wdsd.c.add.lsoa01',
    name   = 'WDSD_CLEAN_ADD_LSOA2001',
    origin = 'SAILX0675V',
    tag    = '_${date}',
    alt    = FALSE
  ),
  wdsd.c.add.lsoa11 = list(
    ref    = 'wdsd.c.add.lsoa11',
    name   = 'WDSD_CLEAN_ADD_LSOA2011',
    origin = 'SAILX0675V',
    tag    = '_${date}',
    alt    = FALSE
  ),
  wdsd.c.add.ralf.lsoa01 = list(
    ref    = 'wdsd.c.add.ralf.lsoa01',
    name   = 'WDSD_CLEAN_ADD_RALF_LSOA2001',
    origin = 'SAILX0675V',
    tag    = '_${date}',
    alt    = FALSE
  ),
  wdsd.c.add.ralf.lsoa11 = list(
    ref    = 'wdsd.c.add.ralf.lsoa11',
    name   = 'WDSD_CLEAN_ADD_RALF_LSOA2011',
    origin = 'SAILX0675V',
    tag    = '_${date}',
    alt    = FALSE
  ),
  wdsd.sc.ar.pers = list(
    ref    = 'wdsd.sc.ar.pers',
    name   = 'WDSD_SINGLE_CLEAN_AR_PERS',
    origin = 'SAILX0675V',
    tag    = '_${date}',
    alt    = FALSE
  ),
  wdsd.sc.geo.wales = list(
    ref    = 'wdsd.sc.geo.wales',
    name   = 'WDSD_SINGLE_CLEAN_GEO_WALES',
    origin = 'SAILX0675V',
    tag    = '_${date}',
    alt    = FALSE
  ),
  wdsd.sc.geo.lsoa01 = list(
    ref    = 'wdsd.sc.geo.lsoa01',
    name   = 'WDSD_SINGLE_CLEAN_GEO_LSOA2001',
    origin = 'SAILX0675V',
    tag    = '_${date}',
    alt    = FALSE
  ),
  wdsd.sc.geo.lsoa11 = list(
    ref    = 'wdsd.sc.geo.lsoa11',
    name   = 'WDSD_SINGLE_CLEAN_GEO_LSOA2011',
    origin = 'SAILX0675V',
    tag    = '_${date}',
    alt    = FALSE
  ),
  wdsd.sc.geo.char.lsoa01 = list(
    ref    = 'wdsd.sc.geo.char.lsoa01',
    name   = 'WDSD_SINGLE_CLEAN_GEO_CHAR_LSOA2001',
    origin = 'SAILX0675V',
    tag    = '_${date}',
    alt    = FALSE
  ),
  wdsd.sc.geo.char.lsoa11 = list(
    ref    = 'wdsd.sc.geo.char.lsoa11',
    name   = 'WDSD_SINGLE_CLEAN_GEO_CHAR_LSOA2011',
    origin = 'SAILX0675V',
    tag    = '_${date}',
    alt    = FALSE
  ),
  wdsd.sc.geo.ralf.lsoa01 = list(
    ref    = 'wdsd.sc.geo.ralf.lsoa01',
    name   = 'WDSD_SINGLE_CLEAN_GEO_RALF_LSOA2001',
    origin = 'SAILX0675V',
    tag    = '_${date}',
    alt    = FALSE
  ),
  wdsd.sc.geo.ralf.lsoa11 = list(
    ref    = 'wdsd.sc.geo.ralf.lsoa11',
    name   = 'WDSD_SINGLE_CLEAN_GEO_RALF_LSOA2011',
    origin = 'SAILX0675V',
    tag    = '_${date}',
    alt    = FALSE
  ),


  # WLGP
  wlgp.prac.list = list(
    ref    = 'wlgp.prac.list',
    name   = 'PRAC_LIST',
    origin = 'SAILWLGPV',
    tag    = '_${date}',
    alt    = 'WLGP'
  ),
  wlgp.pat.alf = list(
    ref    = 'wlgp.pat.alf',
    name   = 'PATIENT_ALF',
    origin = 'SAILWLGPV',
    tag    = '_${date}',
    alt    = 'WLGP'
  ),
  wlgp.c.pat.alf = list(
    ref    = 'wlgp.c.pat.alf',
    name   = 'PATIENT_ALF_CLEANSED',
    origin = 'SAILWLGPV',
    tag    = '_${date}',
    alt    = 'WLGP'
  ),
  wlgp.gp.event = list(
    ref    = 'wlgp.gp.event',
    name   = 'GP_EVENT',
    origin = 'SAILWLGPV',
    tag    = '_${date}',
    alt    = 'WLGP'
  ),
  wlgp.gp.event.alf = list(
    ref    = 'wlgp.gp.event.alf',
    name   = 'GP_EVENT_ALF',
    origin = 'SAILWLGPV',
    tag    = '_${date}',
    alt    = 'WLGP'
  ),
  wlgp.c.gp.event = list(
    ref    = 'wlgp.c.gp.event',
    name   = 'GP_EVENT_CLEANSED',
    origin = 'SAILWLGPV',
    tag    = '_${date}',
    alt    = 'WLGP'
  ),
  wlgp.c.gp.event.alf = list(
    ref    = 'wlgp.c.gp.event.alf',
    name   = 'GP_EVENT_ALF_CLEANSED',
    origin = 'SAILWLGPV',
    tag    = '_${date}',
    alt    = 'WLGP'
  ),
  wlgp.c.gp.reg.med = list(
    ref    = 'wlgp.c.gp.reg.med',
    name   = 'WLGP_CLEAN_GP_REG_MEDIAN',
    origin = 'SAILX0675V',
    tag    = '_${date}',
    alt    = FALSE
  ),
  wlgp.c.gp.prac.incl = list(
    ref    = 'wlgp.c.gp.prac.incl',
    name   = 'WLGP_CLEAN_GP_REG_BY_PRAC_INCLNONSAIL_MEDIAN',
    origin = 'SAILX0675V',
    tag    = '_${date}',
    alt    = FALSE
  ),


  # OPDW
  opdw = list(
    ref    = 'opdw',
    name   = 'OUTPATIENTS',
    origin = 'SAILOPDWV',
    tag    = '_${date}',
    alt    = 'OPDW'
  ),
  opdw.oper = list(
    ref    = 'opdw.oper',
    name   = 'OUTPATIENTS_OPER',
    origin = 'SAILOPDWV',
    tag    = '_${date}',
    alt    = 'OPDW'
  ),
  opdw.diag = list(
    ref    = 'opdw.diag',
    name   = 'OUTPATIENTS_DIAG',
    origin = 'SAILOPDWV',
    tag    = '_${date}',
    alt    = 'OPDW'
  ),


  # NCCH
  ncch.imm = list(
    ref    = 'ncch.imm',
    name   = 'IMM',
    origin = 'SAILNCCHV',
    tag    = '_${date}',
    alt    = 'NCCH'
  ),
  ncch.refr.imm.vac = list(
    ref    = 'ncch.refr.imm.vac',
    name   = 'REFR_IMM_VAC',
    origin = 'SAILNCCHV',
    tag    = '_${date}',
    alt    = 'NCCH'
  ),
  ncch.exam = list(
    ref    = 'ncch.exam',
    name   = 'EXAM',
    origin = 'SAILNCCHV',
    tag    = '_${date}',
    alt    = 'NCCH'
  ),
  ncch.blood.test = list(
    ref    = 'ncch.blood.test',
    name   = 'BLOOD_TEST',
    origin = 'SAILNCCHV',
    tag    = '_${date}',
    alt    = 'NCCH'
  ),
  ncch.path.blood.tests = list(
    ref    = 'ncch.path.blood.tests',
    name   = 'PATH_BLOOD_TESTS',
    origin = 'SAILNCCHV',
    tag    = '_${date}',
    alt    = 'NCCH'
  ),
  ncch.path.spcm.detail = list(
    ref    = 'ncch.path.spcm.detail',
    name   = 'PATH_SPCM_DETAIL',
    origin = 'SAILNCCHV',
    tag    = '_${date}',
    alt    = 'NCCH'
  ),
  ncch.sig.cond = list(
    ref    = 'ncch.sig.cond',
    name   = 'SIG_COND',
    origin = 'SAILNCCHV',
    tag    = '_${date}',
    alt    = 'NCCH'
  ),
  ncch.breast.feeding = list(
    ref    = 'ncch.breast.feeding',
    name   = 'BREAST_FEEDING',
    origin = 'SAILNCCHV',
    tag    = '_${date}',
    alt    = 'NCCH'
  ),
  ncch.child = list(
    ref    = 'ncch.child',
    name   = 'CHILD',
    origin = 'SAILNCCHV',
    tag    = '_${date}',
    alt    = 'NCCH'
  ),
  ncch.child.birth = list(
    ref    = 'ncch.child.birth',
    name   = 'CHILD_BIRTH',
    origin = 'SAILNCCHV',
    tag    = '_${date}',
    alt    = 'NCCH'
  ),
  ncch.child.trust = list(
    ref    = 'ncch.child.trust',
    name   = 'CHILD_TRUST',
    origin = 'SAILNCCHV',
    tag    = '_${date}',
    alt    = 'NCCH'
  ),
  ncch.child.measurement = list(
    ref    = 'ncch.child.measurement',
    name   = 'CHILD_MEASUREMENT_PROGRAM',
    origin = 'SAILNCCHV',
    tag    = '_${date}',
    alt    = 'NCCH'
  ),
  ncch.che.healthy.child = list(
    ref    = 'ncch.che.healthy.child',
    name   = 'CHE_HEALTHYCHILDWALESPROGRAMME',
    origin = 'SAILNCCHV',
    tag    = '_${date}',
    alt    = 'NCCH'
  ),


  # PEDW
  pedw.oper = list(
    ref    = 'pedw.oper',
    name   = 'OPER',
    origin = 'SAILPEDWV',
    tag    = '_${date}',
    alt    = 'PEDW'
  ),
  pedw.diag = list(
    ref    = 'pedw.diag',
    name   = 'DIAG',
    origin = 'SAILPEDWV',
    tag    = '_${date}',
    alt    = 'PEDW'
  ),
  pedw.spell = list(
    ref    = 'pedw.spell',
    name   = 'SPELL',
    origin = 'SAILPEDWV',
    tag    = '_${date}',
    alt    = 'PEDW'
  ),
  pedw.episode = list(
    ref    = 'pedw.episode',
    name   = 'EPISODE',
    origin = 'SAILPEDWV',
    tag    = '_${date}',
    alt    = 'PEDW'
  ),
  pedw.superspell = list(
    ref    = 'pedw.superspell',
    name   = 'SUPERSPELL',
    origin = 'SAILPEDWV',
    tag    = '_${date}',
    alt    = 'PEDW'
  ),
  pedw.admissions = list(
    ref    = 'pedw.admissions',
    name   = 'PEDW_ADMISSIONS',
    origin = 'SAILPEDWV',
    tag    = '_${date}',
    alt    = FALSE
  ),
  pedw.single.diag = list(
    ref    = 'pedw.single.diag',
    name   = 'PEDW_SINGLE_DIAG',
    origin = 'SAILPEDWV',
    tag    = '_${date}',
    alt    = FALSE
  ),
  pedw.single.oper = list(
    ref    = 'pedw.single.oper',
    name   = 'PEDW_SINGLE_OPER',
    origin = 'SAILPEDWV',
    tag    = '_${date}',
    alt    = FALSE
  ),


  # EDDS
  edds = list(
    ref    = 'edds',
    name   = 'EDDS',
    origin = 'SAILEDDSV',
    tag    = '_${date}',
    alt    = 'EDDS'
  ),


  # CCDS
  ccds.crit.care.ep = list(
    ref    = 'ccds.crit.care.ep',
    name   = 'CRITICAL_CARE_EPISODE',
    origin = 'SAILCCDSV',
    tag    = '_${date}',
    alt    = 'CCDS'
  ),


  # PADS
  pads.alf = list(
    ref    = 'pads.alf',
    name   = 'PADS_ALF',
    origin = 'SAILPADSV',
    tag    = '_${date}',
    alt    = 'PADS'
  ),
  pads.lids.rec = list(
    ref    = 'pads.lids.rec',
    name   = 'MATCHED_LIDS_REC_1',
    origin = 'SAILPADSV',
    tag    = '_${date}',
    alt    = 'PADS'
  ),
  pads.lids.dis.new = list(
    ref    = 'pads.lids.dis.new',
    name   = 'MATCHED_LIDS_DIS_NEW',
    origin = 'SAILPADSV',
    tag    = '_${date}',
    alt    = 'PADS'
  ),
  pads.lids.dis.mid = list(
    ref    = 'pads.lids.dis.mid',
    name   = 'MATCHED_LIDS_DIS_MID',
    origin = 'SAILPADSV',
    tag    = '_${date}',
    alt    = 'PADS'
  ),
  pads.lids.dis.old = list(
    ref    = 'pads.lids.dis.old',
    name   = 'MATCHED_LIDS_DIS_OLD',
    origin = 'SAILPADSV',
    tag    = '_${date}',
    alt    = 'PADS'
  ),
  pads.sail.matched = list(
    ref    = 'pads.sail.matched',
    name   = 'POP_SAILMATCHED_1',
    origin = 'SAILPADSV',
    tag    = '_${date}',
    alt    = 'PADS'
  ),


  # WICSU
  wcsu.alf = list(
    ref    = 'wcsu.alf',
    name   = 'WCSISU_ALF',
    origin = 'SAILWCSUV',
    tag    = '_${date}',
    alt    = 'WCSU'
  ),
  wcsu.stage = list(
    ref    = 'wcsu.stage',
    name   = 'STAGE',
    origin = 'SAILWCSUV',
    tag    = '_${date}',
    alt    = 'WCSU'
  ),
  wcsu.treatment = list(
    ref    = 'wcsu.treatment',
    name   = 'TREATMENT',
    origin = 'SAILWCSUV',
    tag    = '_${date}',
    alt    = 'WCSU'
  ),
  wcsu.neoplasm = list(
    ref    = 'wcsu.neoplasm',
    name   = 'NEOPLASM',
    origin = 'SAILWCSUV',
    tag    = '_${date}',
    alt    = 'WCSU'
  ),


  # CNIS
  cnis.wcisu2024 = list(
    ref    = 'cnis.wcisu2024',
    name   = 'WCISU_2024_CNSANDRISKFACTORS',
    origin = 'SAILCNISV',
    tag    = '_${date}',
    alt    = 'CNIS'
  ),
  cnis.mdt = list(
    ref    = 'cnis.mdt',
    name   = 'MDT',
    origin = 'SAILCNISV',
    tag    = '_${date}',
    alt    = 'CNIS'
  ),
  cnis.patient = list(
    ref    = 'cnis.patient',
    name   = 'PATIENT',
    origin = 'SAILCNISV',
    tag    = '_${date}',
    alt    = 'CNIS'
  ),
  cnis.referral = list(
    ref    = 'cnis.referral',
    name   = 'REFERRAL',
    origin = 'SAILCNISV',
    tag    = '_${date}',
    alt    = 'CNIS'
  ),
  cnis.diagnosis = list(
    ref    = 'cnis.diagnosis',
    name   = 'DIAGNOSIS',
    origin = 'SAILCNISV',
    tag    = '_${date}',
    alt    = 'CNIS'
  ),
  cnis.observations = list(
    ref    = 'cnis.observations',
    name   = 'OBSERVATIONS',
    origin = 'SAILCNISV',
    tag    = '_${date}',
    alt    = 'CNIS'
  ),
  cnis.pathology = list(
    ref    = 'cnis.pathology',
    name   = 'PATHOLOGY',
    origin = 'SAILCNISV',
    tag    = '_${date}',
    alt    = 'CNIS'
  ),
  cnis.investigations = list(
    ref    = 'cnis.investigations',
    name   = 'INVESTIGATIONS',
    origin = 'SAILCNISV',
    tag    = '_${date}',
    alt    = 'CNIS'
  ),
  cnis.radiotherapy = list(
    ref    = 'cnis.radiotherapy',
    name   = 'RADIOTHERAPY',
    origin = 'SAILCNISV',
    tag    = '_${date}',
    alt    = 'CNIS'
  ),
  cnis.radiotherapy.end = list(
    ref    = 'cnis.radiotherapy.end',
    name   = 'RADIOTHERAPY_ENDDATE',
    origin = 'SAILCNISV',
    tag    = '_${date}',
    alt    = 'CNIS'
  ),
  cnis.drugtherapy = list(
    ref    = 'cnis.drugtherapy',
    name   = 'DRUGTHERAPY',
    origin = 'SAILCNISV',
    tag    = '_${date}',
    alt    = 'CNIS'
  ),
  cnis.surgery = list(
    ref    = 'cnis.surgery',
    name   = 'SURGERY',
    origin = 'SAILCNISV',
    tag    = '_${date}',
    alt    = 'CNIS'
  ),
  cnis.complications = list(
    ref    = 'cnis.complications',
    name   = 'COMPLICATIONS',
    origin = 'SAILCNISV',
    tag    = '_${date}',
    alt    = 'CNIS'
  ),
  cnis.diseaserecurrence = list(
    ref    = 'cnis.diseaserecurrence',
    name   = 'DISEASERECURRENCE',
    origin = 'SAILCNISV',
    tag    = '_${date}',
    alt    = 'CNIS'
  ),
  cnis.keyworker = list(
    ref    = 'cnis.keyworker',
    name   = 'KEYWORKER',
    origin = 'SAILCNISV',
    tag    = '_${date}',
    alt    = 'CNIS'
  ),


  # Misc
  sailref.wimd2019.sm.area = list(
    ref    = 'sailref.wimd2019.sm.area',
    name   = 'WIMD2019_INDEX_AND_DOMAIN_RANKS_BY_SMALL_AREA',
    origin = 'SAILREFRV',
    tag    = FALSE,
    alt    = FALSE,
    static = TRUE
  )
)
