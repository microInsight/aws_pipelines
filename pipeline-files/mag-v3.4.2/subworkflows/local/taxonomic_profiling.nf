/*
 * Perform per metagenome assembled genome (MAG) taxonomic classification
 * using Kraken2 and Centrifuger, followed by abundance estimation with Bracken,
 * standardisation with Taxpasta, and visualisation with Krona and taxhits plots.
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
include { KRAKEN2_DB_PREPARATION                                                 } from '../../modules/local/kraken2_db_preparation'
include { CENTRIFUGER_GET_DIR                                                    } from '../../modules/local/centrifuger/get_dir/main'
include { TAXPASTA_MERGE                                                         } from '../../modules/nf-core/taxpasta/merge/main'
include { TAXPASTA_STANDARDISE                                                   } from '../../modules/nf-core/taxpasta/standardise/main'
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
    KRAKEN2_DB_PREPARATION(Channel.of(file(params.kraken2_db, checkIfExists: true)))

    // get Centrifuger database path
    CENTRIFUGER_GET_DIR(Channel.of([[id: 'db'], file(params.centrifuger_db, checkIfExists: true)]))

    // Kraken2 & Braken classifications
    KRAKEN2_TAXPROFILING(ch_short_reads, KRAKEN2_DB_PREPARATION.out.db.map { info, db -> db }.dump(tag: 'kraken2_db_preparation'))
    ch_versions = ch_versions.mix(KRAKEN2_TAXPROFILING.out.versions)
    ch_taxa_profiles = ch_taxa_profiles.mix(
        KRAKEN2_TAXPROFILING.out.report.map { meta, report ->
            [meta + [tool: meta.tool == 'bracken' ? 'kraken2-bracken' : meta.tool], report]
        }
    )

    BRACKEN_KRAKEN2(KRAKEN2_TAXPROFILING.out.report, KRAKEN2_DB_PREPARATION.out.db.map { info, db -> db }.dump(tag: 'kraken2_db_preparation'))
    ch_versions = ch_versions.mix(BRACKEN_KRAKEN2.out.versions)
    ch_taxa_profiles = ch_taxa_profiles.mix(BRACKEN_KRAKEN2.out.reports)

    PLOT_KRAKEN2BRACKEN(BRACKEN_KRAKEN2.out.reports, "Kraken2, Bracken", [], ch_taxpasta_tax_dir)
    ch_versions = ch_versions.mix(PLOT_KRAKEN2BRACKEN.out.versions)

    ch_parsedreports = ch_parsedreports.mix(BRACKEN_KRAKEN2.out.reports)

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
    BRACKEN_CENTRIFUGER(CENTRIFUGER_KREPORT.out.kreport, KRAKEN2_DB_PREPARATION.out.db.map { info, db -> db }.dump(tag: 'kraken2_db_preparation'))
    ch_versions = ch_versions.mix(BRACKEN_CENTRIFUGER.out.versions)
    ch_taxa_profiles = ch_taxa_profiles.mix(BRACKEN_CENTRIFUGER.out.reports)

    PLOT_CENTRIFUGERBRACKEN(BRACKEN_CENTRIFUGER.out.reports, "Centrifuger, Bracken", [], ch_taxpasta_tax_dir)
    ch_versions = ch_versions.mix(PLOT_CENTRIFUGERBRACKEN.out.versions)
    ch_parsedreports = ch_parsedreports.mix(BRACKEN_CENTRIFUGER.out.reports)


    // Taxpasta merge and standardise profiling outputs (possibly split into new subworkflow for ease of maintenance in future)
    profiles = ch_taxa_profiles
    // Taxpasta standardisation
    ch_prepare_for_taxpasta = profiles
        .map { meta, profile ->
            def meta_new = [:]
            meta_new.tool = meta.tool
            meta_new.db_name = meta.db_name
            [meta_new, profile]
        }
        .groupTuple()
        .map { meta, input_profiles ->
            meta = meta + [
                tool: meta.tool == 'kraken2-bracken' ? 'kraken2' : meta.tool,
                id: meta.tool == 'kraken2-bracken' ? "${meta.db_name}-bracken" : "${meta.db_name}",
            ]
            [meta, input_profiles.flatten()]
        }
    // We replace kraken2-bracken to kraken2 replace to get the right output-format description (as it's Kraken style)
    // Bracken to id append so to disambiguate when we have same databases for kraken2 step of bracken, with normal bracken

    ch_input_for_taxpasta = ch_prepare_for_taxpasta.branch { _meta, profile ->
        merge: profile.size() > 1
        standardise: true
    }

    ch_input_for_taxpasta_merge = ch_input_for_taxpasta.merge.multiMap { meta, input_profiles ->
        profiles: [meta, input_profiles]
        tool: meta.tool
    }

    ch_input_for_taxpasta_standardise = ch_input_for_taxpasta.standardise.multiMap { meta, input_profiles ->
        profiles: [meta, input_profiles]
        tool: meta.tool
    }


    TAXPASTA_MERGE(ch_input_for_taxpasta_merge.profiles, ch_input_for_taxpasta_merge.tool, 'tsv', ch_taxpasta_tax_dir)
    TAXPASTA_STANDARDISE(ch_input_for_taxpasta_standardise.profiles, ch_input_for_taxpasta_standardise.tool, 'tsv', ch_taxpasta_tax_dir)
    ch_versions = ch_versions.mix(TAXPASTA_MERGE.out.versions.first())
    ch_versions = ch_versions.mix(TAXPASTA_STANDARDISE.out.versions.first())

    /*
        Split profile results based on tool they come from
    */
    ch_input_profiles = profiles.branch {
        bracken: it[0]['tool'] == 'bracken'
        centrifuge: it[0]['tool'] == 'centrifuge'
        kraken2: it[0]['tool'] == 'kraken2' || it[0]['tool'] == 'kraken2-bracken'
        unknown: true
    }

    /*
        Standardise and aggregate
    */

    // Bracken

    ch_profiles_for_bracken = groupProfiles(ch_input_profiles.bracken)

    BRACKEN_COMBINEBRACKENOUTPUTS(ch_profiles_for_bracken)

    // CENTRIFUGER

    // Collect and replace id for db_name for prefix
    // Have to sort by size to ensure first file actually has hits otherwise
    // the script fails
    ch_profiles_for_centrifuger = groupProfiles(
        ch_input_profiles.centrifuge,
        [sort: { -it.size() }],
    )

    KRAKENTOOLS_COMBINEKREPORTS_CENTRIFUGER(ch_profiles_for_centrifuger)
    ch_multiqc_files = ch_multiqc_files.mix(KRAKENTOOLS_COMBINEKREPORTS_CENTRIFUGER.out.txt)
    ch_versions = ch_versions.mix(KRAKENTOOLS_COMBINEKREPORTS_CENTRIFUGER.out.versions)

    PLOT_CENTRIFUGER(
        TAXPASTA_STANDARDISE.out.standardised_profile,
        "Centrifuger, Taxpasta",
        [],
        ch_taxpasta_tax_dir
    )
    ch_versions = ch_versions.mix(PLOT_CENTRIFUGER.out.versions)

    // Kraken2

    // Collect and replace id for db_name for prefix
    // Have to sort by size to ensure first file actually has hits otherwise
    // the script fails
    ch_profiles_for_kraken2 = groupProfiles(
        ch_input_profiles.kraken2.map { meta, profile ->
            // Replace database name, to get the right output description.
            def db_name = meta.tool == 'kraken2-bracken' ? "${meta.db_name}-bracken" : "${meta.db_name}"
            return [meta + [db_name: db_name], profile]
        },
        [sort: { -it.size() }],
    )

    PLOT_KRAKEN2(
        TAXPASTA_STANDARDISE.out.standardised_profile,
        "Kraken2, Taxpasta",
        [],
        ch_taxpasta_tax_dir
    )
    ch_versions = ch_versions.mix(PLOT_KRAKEN2.out.versions)


    KRAKENTOOLS_COMBINEKREPORTS_KRAKEN(ch_profiles_for_kraken2)
    ch_multiqc_files = ch_multiqc_files.mix(KRAKENTOOLS_COMBINEKREPORTS_KRAKEN.out.txt)
    ch_versions = ch_versions.mix(KRAKENTOOLS_COMBINEKREPORTS_KRAKEN.out.versions)


    ch_in_1 = TAXPASTA_STANDARDISE.out.standardised_profile.join(TAXPASTA_STANDARDISE.out.standardised_profile, by: [0])
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
    ch_taxreports      = ch_parsedreports.groupTuple(by: [0])
    ch_taxpasta_report = TAXPASTA_MERGE.out.merged_profiles
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
