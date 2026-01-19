/*
 * Perform per metagenome assembled genome (MAG) taxonomic classification
 * using Kraken2 and Centrifuger, followed by abundance estimation with Bracken,
 * and visualisation with Krona and taxhits plots.
 */

include { KRAKEN2 as KRAKEN2_TAXPROFILING                                        } from '../../modules/local/kraken2'
include { BRACKEN_BRACKEN as BRACKEN                                             } from '../../modules/nf-core/bracken/bracken/main'
include { TAXPASTA_STANDARDISE as TAXPASTA_STANDARDISE_KRAKEN2                   } from '../../modules/nf-core/taxpasta/standardise/main'
include { TAXPASTA_STANDARDISE as TAXPASTA_STANDARDISE_CENTRIFUGER               } from '../../modules/nf-core/taxpasta/standardise/main'
include { CENTRIFUGER_CENTRIFUGER                                                } from '../../modules/local/centrifuger/centrifuger/main'
include { CENTRIFUGER_KREPORT                                                    } from '../../modules/local/centrifuger/kreport/main'
include { PLOT_TAXHITS                                                           } from '../../modules/local/plot_taxhits'
include { PLOT_INDVTAXHITS as PLOT_KRAKEN2                                       } from '../../modules/local/plot_indvtaxhits'
include { PLOT_INDVTAXHITS as PLOT_CENTRIFUGER                                   } from '../../modules/local/plot_indvtaxhits'
include { PLOT_INDVTAXHITS as PLOT_KRAKEN2BRACKEN                                } from '../../modules/local/plot_indvtaxhits'
include { PLOT_INDVTAXHITS as PLOT_CENTRIFUGERBRACKEN                            } from '../../modules/local/plot_indvtaxhits'
include { KRONA_KTIMPORTTAXONOMY                                                 } from '../../modules/nf-core/krona/ktimporttaxonomy/main'
include { KRAKENTOOLS_KREPORT2KRONA                                              } from '../../modules/nf-core/krakentools/kreport2krona/main'
include { UNTAR                                                                  } from '../../modules/nf-core/untar/main'
include { CENTRIFUGER_GET_DIR                                                    } from '../../modules/local/centrifuger/get_dir/main'
include { BRACKEN_COMBINEBRACKENOUTPUTS                                          } from '../../modules/nf-core/bracken/combinebrackenoutputs/main'
include { KRAKENTOOLS_COMBINEKREPORTS as KRAKENTOOLS_COMBINEKREPORTS_KRAKEN      } from '../../modules/nf-core/krakentools/combinekreports/main'
include { KRAKENTOOLS_COMBINEKREPORTS as KRAKENTOOLS_COMBINEKREPORTS_CENTRIFUGER } from '../../modules/nf-core/krakentools/combinekreports/main'



workflow TAXONOMIC_PROFILING {
    take:
    ch_short_reads

    main:
    ch_versions = Channel.empty()
    ch_multiqc_files = Channel.empty()
    ch_parsedreports = Channel.empty()
    ch_taxa_profiles = Channel.empty()
    ch_plot_reports = Channel.empty()

    // split GTDB R226 taxonomic information for taxpasta standardisation
    ch_taxpasta_tax_dir = params.taxpasta_taxonomy_dir ? Channel.fromPath(params.taxpasta_taxonomy_dir, checkIfExists: true) : []

    // untar Kraken2 databases from .tar.gz file input and pull out the k2_database folder
    ch_kraken2_db = file(params.kraken2_db, checkifExists: true)
    if (ch_kraken2_db.name.endsWith( ".tar.gz" )) {
            Channel
            .value(ch_kraken2_db)
            .map {
                kraken2_db -> [
                    ['id' : 'kraken2_database'],
                    kraken2_db
                    ]
                }
                .set { archive }

            UNTAR ( archive )

            k2_database    = UNTAR.out.untar.map{ it -> it[1] }
            ch_versions = ch_versions.mix(UNTAR.out.versions.first())

    } else {
            k2_database = Channel.fromPath(params.kraken2_db)
    }

    // get Centrifuger database path
    CENTRIFUGER_GET_DIR(Channel.of([[id: 'db'], file(params.centrifuger_db, checkIfExists: true)]))

    // Kraken2 & Braken classifications - Bracken results summarized at species level (S) [may change in future or be parameterised]
    KRAKEN2_TAXPROFILING(ch_short_reads, k2_database)
    ch_versions = ch_versions.mix(KRAKEN2_TAXPROFILING.out.versions)
    ch_taxa_profiles = ch_taxa_profiles.mix(
        KRAKEN2_TAXPROFILING.out.report.map { meta, report ->
            [meta + [tool: 'kraken2'] + [classifier: 'kraken2'], report]
        }
    )

    TAXPASTA_STANDARDISE_KRAKEN2(KRAKEN2_TAXPROFILING.out.report, 'kraken2', 'tsv', ch_taxpasta_tax_dir)
    ch_plot_reports = ch_plot_reports.mix(TAXPASTA_STANDARDISE_KRAKEN2.out.standardised_profile)
    ch_versions = ch_versions.mix(TAXPASTA_STANDARDISE_KRAKEN2.out.versions)

    // Centrifuger taxonomic profiling
    CENTRIFUGER_CENTRIFUGER(ch_short_reads, CENTRIFUGER_GET_DIR.out.untar)
    ch_centrifuger_results = CENTRIFUGER_CENTRIFUGER.out.results.mix(
        CENTRIFUGER_CENTRIFUGER.out.results.map { meta, result ->
            [meta + [tool: 'centrifuger'] + [classifier: 'centrifuger'], result]
        }
    )
    ch_versions = ch_versions.mix(CENTRIFUGER_CENTRIFUGER.out.versions)

    CENTRIFUGER_KREPORT(ch_centrifuger_results, CENTRIFUGER_GET_DIR.out.untar)
    ch_versions = ch_versions.mix(CENTRIFUGER_KREPORT.out.versions)
    ch_taxa_profiles = ch_taxa_profiles.mix(
        CENTRIFUGER_KREPORT.out.kreport.map { meta, report ->
            [meta + [tool: 'centrifuge'] + [classifier: 'centrifuger'], report]
        }
    )
    ch_plot_reports = ch_plot_reports.mix(CENTRIFUGER_KREPORT.out.kreport)
    ch_parsedreports = ch_parsedreports.mix(CENTRIFUGER_KREPORT.out.kreport)

    // Bracken on Centrifuger Kraken-style outputs & Kraken2 outputs
    ch_bracken_input = KRAKEN2_TAXPROFILING.out.report
        .map { meta, report ->
            [meta + [tool: 'kraken2'] + [classifier: 'kraken2'], report]
        }
    ch_bracken_input = ch_bracken_input
        .mix(CENTRIFUGER_KREPORT.out.kreport
            .map { meta, report ->
                [meta + [tool: 'centrifuge'] + [classifier: 'centrifuger'], report]
            }
        )

    BRACKEN(ch_bracken_input, k2_database, 'S')
    ch_versions = ch_versions.mix(BRACKEN.out.versions)

    ch_bracken_results = BRACKEN.out.reports
        .map { meta, report ->
        def br_tool = meta.classifier == 'kraken2' ? 'kraken2-bracken' : 'centrifuge-bracken'
            [meta + [tool: br_tool] + [classifier: meta.classifier], report]
        }
    ch_taxa_profiles = ch_taxa_profiles.mix(ch_bracken_results)
    ch_plot_reports = ch_plot_reports.mix(BRACKEN.out.reports)

    ch_bracken_plot_input = ch_bracken_results
        .branch { _meta, _report, classifier ->
            kraken2: classifier == 'kraken2'
            centrifuger: classifier == 'centrifuger'
        }

    PLOT_KRAKEN2BRACKEN(
        ch_bracken_plot_input.kraken2,
        "Kraken2, Bracken",
        Channel.value(params.tax_prof_gtdb_metadata),
        file("/mnt/workflow/definition/mag-v3.4.2/docs/images/mi_logo.png"),
        file(params.tax_prof_template, checkIfExists: true)
    )
    ch_parsedreports = ch_parsedreports.mix(BRACKEN.out.reports)

    PLOT_CENTRIFUGERBRACKEN(
        ch_bracken_plot_input.centrifuger,
        "Centrifuger, Bracken",
        Channel.value(file(params.tax_prof_gtdb_metadata, checkIfExists: true)),
        file("/mnt/workflow/definition/mag-v3.4.2/docs/images/mi_logo.png"),
        file(params.tax_prof_template, checkIfExists: true)
    )
    ch_parsedreports = ch_parsedreports.mix(BRACKEN.out.reports)

    TAXPASTA_STANDARDISE_CENTRIFUGER(CENTRIFUGER_KREPORT.out.kreport, 'centrifuge', 'tsv', ch_taxpasta_tax_dir)
    ch_versions = ch_versions.mix(TAXPASTA_STANDARDISE_CENTRIFUGER.out.versions)
    ch_plot_reports = ch_plot_reports.mix(TAXPASTA_STANDARDISE_CENTRIFUGER.out.standardised_profile)

    PLOT_CENTRIFUGER(
        TAXPASTA_STANDARDISE_CENTRIFUGER.out.standardised_profile,
        "Centrifuger, Taxpasta",
        Channel.value(file(params.tax_prof_gtdb_metadata, checkIfExists: true)),
        file("/mnt/workflow/definition/mag-v3.4.2/docs/images/mi_logo.png"),
        file(params.tax_prof_template, checkIfExists: true)
        )

    ch_taxhits_input = ch_plot_reports
        .groupTuple(size: 4)
        .map { id, reports ->
            def taxpasta_kraken = reports.find { meta, _report -> meta.tool == 'taxpasta-kraken2' }?.getAt(1)
            def taxpasta_centrifuger = reports.find { meta, _report -> meta.tool == 'taxpasta-centrifuge' }?.getAt(1)
            def kraken2_bracken = reports.find {  meta, _report -> meta.tool == 'kraken2-bracken' }?.getAt(1)
            def centrifuger_bracken = reports.find {  meta, _report -> meta.tool == 'centrifuge-bracken' }?.getAt(1)
            def meta = [id: id]
            [meta, taxpasta_kraken, taxpasta_centrifuger, kraken2_bracken, centrifuger_bracken]
        }

    PLOT_TAXHITS(
        ch_taxhits_input,
        file(params.tax_prof_gtdb_metadata, checkIfExists: true),
        file("/mnt/workflow/definition/mag-v3.4.2/docs/images/mi_logo.png"),
        file(params.tax_prof_template, checkIfExists: true)
        )


    // Join together for Krona visualisation
    KRAKENTOOLS_KREPORT2KRONA(BRACKEN.out.txt)

    KRONA_KTIMPORTTAXONOMY(
        KRAKENTOOLS_KREPORT2KRONA.out.txt,
        file(params.krona_db, checkIfExists: true),
    )
    ch_versions = ch_versions.mix(KRONA_KTIMPORTTAXONOMY.out.versions)

    emit:
    profiles           = ch_taxa_profiles
    ch_taxreports      = ch_parsedreports
    ch_kreports        = ch_plot_reports
    ch_multiqc         = ch_multiqc_files
    versions           = ch_versions
}

// Custom Functions

/**
* Group all profiles per reference database.
*
* @param ch_profiles A channel containing pairs of a meta map and the report of
*   a given profiler, where meta must contain a key `db_name`.
* @return A channel with one element per reference database. Each element is a
*   pair of a meta map with an `id` key and all corresponding profiles.
*/
def groupProfiles(ch_profiles, groupTupleOptions = [:]) {
    return ch_profiles
        .map { meta, profile -> [meta.db_name, profile] }
        .groupTuple(groupTupleOptions)
        .map { db_name, profiles -> [[id: db_name], profiles] }
}
