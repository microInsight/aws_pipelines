//
// Run KOFAMSCAN on protein fasta from orf_caller output
//
include { KOFAMSCAN_DOWNLOAD } from '../../../modules/local/kofamscan/download/main'
include { KOFAMSCAN_SCAN     } from '../../../modules/local/kofamscan/scan/main'
include { KOFAMSCAN_UNIQUE   } from '../../../modules/local/kofamscan/unique/main'
include { KOFAMSCAN_SUM      } from '../../../modules/local/kofamscan/sum/main'

workflow KOFAMSCAN {
    take:
    kofamscan // Channel: val(meta), path(fasta)
    fcs       // featureCounts output

    main:
    ch_versions = Channel.empty()

    // Check if kofam_dir is provided, if so use S3 files, otherwise download
    if (params.kofam_dir) {
        ch_ko_list = Channel.fromPath("${params.kofam_dir}/ko_list")
        ch_koprofiles = Channel.fromPath("${params.kofam_dir}/profiles/prokaryote.hal")
    } else {
        KOFAMSCAN_DOWNLOAD()
        ch_ko_list = KOFAMSCAN_DOWNLOAD.out.ko_list
        ch_koprofiles = KOFAMSCAN_DOWNLOAD.out.koprofiles
    }

    KOFAMSCAN_SCAN( kofamscan, ch_ko_list, ch_koprofiles )
    ch_versions = ch_versions.mix(KOFAMSCAN_SCAN.out.versions)

    KOFAMSCAN_UNIQUE(KOFAMSCAN_SCAN.out.kofamtsv)
    ch_versions = ch_versions.mix(KOFAMSCAN_UNIQUE.out.versions)

    KOFAMSCAN_SUM( KOFAMSCAN_SCAN.out.kout, fcs )
    ch_versions = ch_versions.mix(KOFAMSCAN_SUM.out.versions)

    emit:
    kofam_table_out   = KOFAMSCAN_SCAN.out.kout
    kofam_table_tsv   = KOFAMSCAN_SCAN.out.kofamtsv
    kofam_table_uniq  = KOFAMSCAN_UNIQUE.out.kofamuniq
    kofamscan_summary = KOFAMSCAN_SUM.out.kofamscan_summary
    versions          = ch_versions
}
