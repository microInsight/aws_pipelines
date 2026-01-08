/*
 * Perform per metagenome assembled genome (MAG) taxonomic classification
 * using Kraken2 and Centrifuger, followed by abundance estimation with Bracken,
 * and visualisation with Krona and taxhits plots.
 */

include { KRAKEN2 as KRAKEN2_TAXPROFILING                                        } from '../../modules/local/kraken2'
include { BRACKEN_BRACKEN as BRACKEN_KRAKEN2                                     } from '../../modules/nf-core/bracken/bracken/main'
include { BRACKEN_BRACKEN as BRACKEN_CENTRIFUGER                                 } from '../../modules/nf-core/bracken/bracken/main'
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

    // split GTDB R226 taxonomic information for taxpasta standardisation
    ch_taxpasta_tax_dir = params.taxpasta_taxonomy_dir ? Channel.fromPath(params.taxpasta_taxonomy_dir, checkIfExists: true).collect() : []

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
            k2_database = ch_kraken2_db
    }

    // get Centrifuger database path
    CENTRIFUGER_GET_DIR(Channel.of([[id: 'db'], file(params.centrifuger_db, checkIfExists: true)]))

    // Kraken2 & Braken classifications
    KRAKEN2_TAXPROFILING(ch_short_reads, k2_database)
    ch_versions = ch_versions.mix(KRAKEN2_TAXPROFILING.out.versions)
    ch_taxa_profiles = ch_taxa_profiles.mix(
        KRAKEN2_TAXPROFILING.out.report.map { meta, report ->
            [meta + [tool: meta.tool == 'bracken' ? 'kraken2-bracken' : meta.tool], report]
        }
    )

    BRACKEN_KRAKEN2(KRAKEN2_TAXPROFILING.out.report, k2_database)
    ch_versions = ch_versions.mix(BRACKEN_KRAKEN2.out.versions)
    ch_taxa_profiles = ch_taxa_profiles.mix(BRACKEN_KRAKEN2.out.reports)

    PLOT_KRAKEN2BRACKEN(BRACKEN_KRAKEN2.out.reports, "Kraken2, Bracken", [], ch_taxpasta_tax_dir)
    ch_versions = ch_versions.mix(PLOT_KRAKEN2BRACKEN.out.versions)

    ch_parsedreports = ch_parsedreports.mix(BRACKEN_KRAKEN2.out.reports)

    TAXPASTA_STANDARDISE_KRAKEN2(KRAKEN2_TAXPROFILING.out.report, 'kraken2', 'tsv', ch_taxpasta_tax_dir)
            ch_versions = ch_versions.mix(TAXPASTA_STANDARDISE_KRAKEN2.out.versions)

    // Centrifuger taxonomic profiling
    CENTRIFUGER_CENTRIFUGER(ch_short_reads, CENTRIFUGER_GET_DIR.out.untar)
    ch_versions = ch_versions.mix(CENTRIFUGER_CENTRIFUGER.out.versions)

    CENTRIFUGER_KREPORT(CENTRIFUGER_CENTRIFUGER.out.results, CENTRIFUGER_GET_DIR.out.untar)
    ch_versions = ch_versions.mix(CENTRIFUGER_KREPORT.out.versions)
    ch_taxa_profiles = ch_taxa_profiles.mix(
        CENTRIFUGER_KREPORT.out.kreport.map { meta, report ->
            [meta + [tool: 'centrifuge'], report]
        }
    )
    ch_parsedreports = ch_parsedreports.mix(CENTRIFUGER_KREPORT.out.kreport)

    // Bracken on Centrifuger Kraken-style outputs
    BRACKEN_CENTRIFUGER(CENTRIFUGER_KREPORT.out.kreport, k2_database)
    ch_versions = ch_versions.mix(BRACKEN_CENTRIFUGER.out.versions)
    ch_taxa_profiles = ch_taxa_profiles.mix(BRACKEN_CENTRIFUGER.out.reports)

    PLOT_CENTRIFUGERBRACKEN(BRACKEN_CENTRIFUGER.out.reports, "Centrifuger, Bracken", [], ch_taxpasta_tax_dir)
    ch_versions = ch_versions.mix(PLOT_CENTRIFUGERBRACKEN.out.versions)
    ch_parsedreports = ch_parsedreports.mix(BRACKEN_CENTRIFUGER.out.reports)

    TAXPASTA_STANDARDISE_CENTRIFUGER(CENTRIFUGER_KREPORT.out.kreport, 'centrifuge', 'tsv', ch_taxpasta_tax_dir)
            ch_versions = ch_versions.mix(TAXPASTA_STANDARDISE_CENTRIFUGER.out.versions)

    PLOT_CENTRIFUGER(TAXPASTA_STANDARDISE_CENTRIFUGER.out.standardised_profile, "Centrifuger, Taxpasta", [], params.TAXONOMIC_PROFILING.template)
            ch_versions = ch_versions.mix(PLOT_CENTRIFUGER.out.versions)

    ch_in_1 = TAXPASTA_STANDARDISE_KRAKEN2.out.standardised_profile.join(TAXPASTA_STANDARDISE_CENTRIFUGER.out.standardised_profile, by: [0])
    ch_in_3 = ch_in_1.join(BRACKEN_KRAKEN2.out.reports, by: [0])
    ch_in_4 = ch_in_3.join(BRACKEN_CENTRIFUGER.out.reports, by: [0])

    PLOT_TAXHITS(ch_in_4, params.tax_prof_gtdb_metadata, file(params.tax_prof_template, checkIfExists: true))
    ch_versions = ch_versions.mix(PLOT_TAXHITS.out.versions)


    // Join together for Krona
    ch_tax_classifications = BRACKEN_KRAKEN2.out.txt
        .mix(BRACKEN_CENTRIFUGER.out.txt)
        .map { classifier, meta, report ->
            def meta_new = meta + [classifier: classifier]
            [meta_new, report]
        }

    KRAKENTOOLS_KREPORT2KRONA(ch_tax_classifications)

    KRONA_KTIMPORTTAXONOMY(
        KRAKENTOOLS_KREPORT2KRONA.out.txt,
        file(params.krona_db, checkIfExists: true),
    )
    ch_versions = ch_versions.mix(KRONA_KTIMPORTTAXONOMY.out.versions)

    emit:
    profiles           = ch_taxa_profiles
    ch_taxreports      = ch_parsedreports.groupTuple(by: [0])
    ch_kreports        = ch_in_4.groupTuple(by: [0])
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
