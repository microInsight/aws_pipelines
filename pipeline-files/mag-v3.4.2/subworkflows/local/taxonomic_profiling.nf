/*
 * Perform per metagenome assembled genome (MAG) quality control and typing
 */

include { KRAKEN2 as KRAKEN2_TAXPROFILING                          } from '../../modules/local/kraken2'
include { BRACKEN_BRACKEN as BRACKEN_KRAKEN2                       } from '../../modules/nf-core/bracken/bracken/main'
include { BRACKEN_BRACKEN as BRACKEN_CENTRIFUGER                   } from '../../modules/nf-core/bracken/bracken/main'
include { KRAKEN2_DB_PREPARATION                                   } from '../../modules/local/kraken2_db_preparation'
include { TAXPASTA_STANDARDISE as TAXPASTA_STANDARDISE_KRAKEN2     } from '../../modules/nf-core/taxpasta/standardise/main'
include { TAXPASTA_STANDARDISE as TAXPASTA_STANDARDISE_CENTRIFUGER } from '../../modules/nf-core/taxpasta/standardise/main'
include { CENTRIFUGER_CENTRIFUGER                                  } from '../../modules/local/centrifuger/centrifuger/main'
include { CENTRIFUGER_KREPORT                                      } from '../../modules/local/centrifuger/kreport/main'
include { PLOT_TAXHITS                                             } from '../../modules/local/plot_taxhits'
include { PLOT_INDVTAXHITS as PLOT_KRAKEN2                         } from '../../modules/local/plot_indvtaxhits'
include { PLOT_INDVTAXHITS as PLOT_CENTRIFUGER                     } from '../../modules/local/plot_indvtaxhits'
include { PLOT_INDVTAXHITS as PLOT_KRAKEN2BRACKEN                  } from '../../modules/local/plot_indvtaxhits'
include { PLOT_INDVTAXHITS as PLOT_CENTRIFUGERBRACKEN              } from '../../modules/local/plot_indvtaxhits'
include { KRONA_KTIMPORTTAXONOMY                                   } from '../../modules/nf-core/krona/ktimporttaxonomy/main'

workflow TAXONOMIC_PROFILING {
    take:
    ch_short_reads
    ch_kraken2_db_file
    krona_db
    ch_tax_prof_dbdir

    main:
    ch_versions = Channel.empty()
    ch_parsedreports = Channel.empty()


    if (ch_kraken2_db_file.extension in ['gz', 'tgz']) {
        // Expects to be tar.gz!
        ch_db_for_kraken2 = KRAKEN2_DB_PREPARATION(ch_kraken2_db_file).db
        }
        else if (ch_kraken2_db_file.isDirectory()) {
            ch_db_for_kraken2 = Channel.fromPath("${params.ch_kraken2_db_file}/*.k2d")
                .collect()
                .map { file ->
                    if (file.size() >= 3) {
                        def db_name = file[0].getParent().getName()
                        [db_name, file]
                    }
                    else {
                        error("Kraken2 requires '{hash,opts,taxo}.k2d' files.")
                    }
                }
            }

   centrifuger_db = Channel.fromPath(ch_tax_prof_dbdir)

    if (ch_kraken2_db_file) {
        KRAKEN2_TAXPROFILING(ch_short_reads, ch_kraken2_db_file, false)
        ch_versions = ch_versions.mix(KRAKEN2_TAXPROFILING.out.versions)

        BRACKEN_KRAKEN2(KRAKEN2_TAXPROFILING.out.report, ch_kraken2_db_file)
        ch_versions = ch_versions.mix(BRACKEN_KRAKEN2.out.versions)

        PLOT_KRAKEN2BRACKEN(BRACKEN_KRAKEN2.out.reports, "Kraken2, Bracken", [], params.tax_prof_template)
        ch_versions = ch_versions.mix(PLOT_KRAKEN2BRACKEN.out.versions)

        if (centrifuger_db) {
            TAXPASTA_STANDARDISE_KRAKEN2(KRAKEN2_TAXPROFILING.out.report, centrifuger_db)
            ch_versions = ch_versions.mix(TAXPASTA_STANDARDISE_KRAKEN2.out.versions)

            PLOT_KRAKEN2(TAXPASTA_STANDARDISE_KRAKEN2.out.standardised_profile, "Kraken2, Taxpasta", [], params.tax_prof_template)
            ch_versions = ch_versions.mix(PLOT_KRAKEN2.out.versions)

            ch_parsedreports = ch_parsedreports.mix(BRACKEN_KRAKEN2.out.reports)

        }
    }

    if (centrifuger_db) {
        CENTRIFUGER_CENTRIFUGER(ch_short_reads, centrifuger_db)
        ch_versions = ch_versions.mix(CENTRIFUGER_CENTRIFUGER.out.versions)

        CENTRIFUGER_KREPORT(CENTRIFUGER_CENTRIFUGER.out.results, centrifuger_db)
        ch_versions = ch_versions.mix(CENTRIFUGER_KREPORT.out.versions)

        ch_parsedreports = ch_parsedreports.mix(CENTRIFUGER_KREPORT.out.kreport)

        if (ch_db_for_kraken2) {
            BRACKEN_CENTRIFUGER(CENTRIFUGER_KREPORT.out.kreport, ch_db_for_kraken2)
            ch_versions = ch_versions.mix(BRACKEN_CENTRIFUGER.out.versions)

            PLOT_CENTRIFUGERBRACKEN(BRACKEN_CENTRIFUGER.out.reports, "Centrifuger, Bracken", [], params.tax_prof_template)
            ch_versions = ch_versions.mix(PLOT_CENTRIFUGERBRACKEN.out.versions)

            ch_parsedreports = ch_parsedreports.mix(BRACKEN_CENTRIFUGER.out.reports)
        }

        if (ch_tax_prof_dbdir) {
            TAXPASTA_STANDARDISE_CENTRIFUGER(CENTRIFUGER_KREPORT.out.kreport, centrifuger_db)
            ch_versions = ch_versions.mix(TAXPASTA_STANDARDISE_CENTRIFUGER.out.versions)

            PLOT_CENTRIFUGER(TAXPASTA_STANDARDISE_CENTRIFUGER.out.standardised_profile, "Centrifuger, Taxpasta", [], params.tax_prof_template)
            ch_versions = ch_versions.mix(PLOT_CENTRIFUGER.out.versions)
        }
    }
    if (ch_db_for_kraken2 && centrifuger_db) {
        ch_in_1 = TAXPASTA_STANDARDISE_KRAKEN2.out.standardised_profile.join(TAXPASTA_STANDARDISE_CENTRIFUGER.out.standardised_profile, by: [0])
        ch_in_3 = ch_in_1.join(BRACKEN_KRAKEN2.out.reports, by: [0])
        ch_in_4 = ch_in_3.join(BRACKEN_CENTRIFUGER.out.reports, by: [0])

        PLOT_TAXHITS(ch_in_4, params.tax_prof_gtdb_metadata, params.tax_prof_template)
        ch_versions = ch_versions.mix(PLOT_TAXHITS.out.versions)

    }

    // Join together for Krona
    if (!params.skip_krona) {
        ch_krona_db = Channel.fromPath(params.krona_db)

        ch_tax_classifications = BRACKEN_KRAKEN2.out.txt
            .mix(BRACKEN_CENTRIFUGER.out.txt)
            .map { classifier, meta, report ->
                def meta_new = meta + [classifier: classifier]
                [meta_new, report]
            }

        KRONA_KTIMPORTTAXONOMY(
            ch_tax_classifications,
            ch_krona_db,
        )
        ch_versions = ch_versions.mix(KRONA_KTIMPORTTAXONOMY.out.versions.first())

    }

    emit:
    ch_taxreports    = ch_parsedreports.groupTuple(by: [0])
    ch_kreports      = ch_in_4.groupTuple(by: [0])
    versions         = ch_versions
}
