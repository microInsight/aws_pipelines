// Taxpasta merge and standardise profiling outputs

include { PLOT_INDVTAXHITS as PLOT_KRAKEN2                                       } from '../../modules/local/plot_indvtaxhits'
include { PLOT_INDVTAXHITS as PLOT_CENTRIFUGER                                   } from '../../modules/local/plot_indvtaxhits'
include { PLOT_INDVTAXHITS as PLOT_KRAKEN2BRACKEN                                } from '../../modules/local/plot_indvtaxhits'
include { PLOT_INDVTAXHITS as PLOT_CENTRIFUGERBRACKEN                            } from '../../modules/local/plot_indvtaxhits'
include { TAXPASTA_MERGE as CENTRIFUGER_MERGE                                    } from '../../modules/nf-core/taxpasta/merge/main'
include { TAXPASTA_MERGE as KRAKEN2BRACKEN_MERGE                                 } from '../../modules/nf-core/taxpasta/merge/main'
include { TAXPASTA_STANDARDISE as CENTRIFUGER_STANDARDISE                        } from '../../modules/nf-core/taxpasta/standardise/main'
include { TAXPASTA_STANDARDISE as KRAKEN2BRACKEN_STANDARDISE                     } from '../../modules/nf-core/taxpasta/standardise/main'
include { BRACKEN_COMBINEBRACKENOUTPUTS                                          } from '../../modules/nf-core/bracken/combinebrackenoutputs/main'
include { KRAKENTOOLS_COMBINEKREPORTS as KRAKENTOOLS_COMBINEKREPORTS_KRAKEN      } from '../../modules/nf-core/krakentools/combinekreports/main'
include { KRAKENTOOLS_COMBINEKREPORTS as KRAKENTOOLS_COMBINEKREPORTS_CENTRIFUGER } from '../../modules/nf-core/krakentools/combinekreports/main'

// Custom Functions

/**
* Group all profiles per classification tool.
*
* @param ch_profiles A channel containing pairs of a meta map and the report of
*   a given profiler, where meta must contain a key `db_name`.
* @return A channel with one element per classification tool. Each element is a
*   pair of a meta map with an `id` key and all corresponding profiles.
*/
def groupProfiles(ch_profiles, groupTupleOptions = [:]) {
    return ch_profiles
        .map { meta, profile -> [meta.tool, profile] }
        .groupTuple(groupTupleOptions)
        .map { tool, profiles -> [[id: tool], profiles] }
}

workflow TAXONOMIC_STANDARDISATION {
    take:
    profiles
    k2_taxonomy_db

    main:
    ch_versions = Channel.empty()
    ch_multiqc_files = Channel.empty()
    ch_merge_reports = Channel.empty()

    // Taxpasta standardisation
    ch_prepare_for_taxpasta = profiles
        .map { meta, profile ->
            def projparse = /^(\d{3}[A-Z]{2,3})/
            def matches = (meta.id =~ projparse)
                [meta + [project: matches[0][1]], file(profile)]
        }
        .map { meta, profile ->
            if (meta.tool =~ /(bracken)/){
                [meta + [tool: "bracken"], file(profile)]
            }
            else if(meta.tool == /centrifuger/) {
                [meta + [tool: "centrifuge"], file(profile)]
            }
            else {
                [meta, file(profile)]
            }
        }
        .map { meta, files ->
            [meta.subMap(['project', 'id', 'tool']), file(files)]
        }
        .branch { meta, _file ->
            kraken2: meta.tool == "kraken2"
            bracken: meta.tool == "bracken"
            centrifuge: meta.tool == "centrifuge"
        }

    ch_input_for_taxpasta = ch_prepare_for_taxpasta.kraken2
        .mix(ch_prepare_for_taxpasta.bracken)
        .mix(ch_prepare_for_taxpasta.centrifuge)
        .groupTuple(by: [0, 2])
        .map { meta, txt_files ->
            [
                meta,
                txt_files.collect { txt -> txt.txt }

            ]
        }

    // split GTDB R226 taxonomic information for taxpasta standardisation
    ch_taxpasta_tax_dir = params.taxpasta_taxonomy_dir ? Channel.fromPath(file(params.taxpasta_taxonomy_dir, checkIfExists: true)).first() : []

    ch_taxpasta_in = ch_input_for_taxpasta
        .multiMap { meta, taxa_profiles ->
            taxa_profiles: [meta, taxa_profiles]
            tool: meta.tool
        }

    // Centrifuge - GTDB taxonomy
    CENTRIFUGER_MERGE(
        ch_taxpasta_in.taxa_profiles.filter { meta, report ->
            report.size() > 1 && meta.tool == "centrifuge"
        },
        'tsv',
        ch_taxpasta_tax_dir
    )
    ch_versions = ch_versions.mix(CENTRIFUGER_MERGE.out.versions)

    // Kraken2 & Bracken - NCBI taxonomy
    KRAKEN2BRACKEN_MERGE(
        ch_taxpasta_in.taxa_profiles.filter { meta, file ->
            file.size() > 1 && (meta.tool == "kraken2" || meta.tool == "bracken")
        },
        'tsv',
        k2_taxonomy_db
        )
    ch_versions = ch_versions.mix(KRAKEN2BRACKEN_MERGE.out.versions)

    ch_merge_reports = ch_merge_reports
        .mix(CENTRIFUGER_MERGE.out.merged_profiles)
        .mix(KRAKEN2BRACKEN_MERGE.out.merged_profiles)

    CENTRIFUGER_STANDARDISE(
        ch_taxpasta_in.taxa_profiles.filter { meta, _report ->
            meta.tool == "centrifuge"
        },
        'tsv',
        ch_taxpasta_tax_dir
    )
    ch_versions = ch_versions.mix(CENTRIFUGER_STANDARDISE.out.versions)

    KRAKEN2BRACKEN_STANDARDISE(
        ch_taxpasta_in.taxa_profiles.filter { meta, _report ->
            meta.tool == "kraken2" || meta.tool == "bracken"
        },
        'tsv',
        k2_taxonomy_db
    )
    ch_versions = ch_versions.mix(KRAKEN2BRACKEN_STANDARDISE.out.versions)


    // Split profile results based on tool they come from

    ch_input_profiles = profiles
        .branch { meta, _profile ->
            bracken: meta.tool =~ /(bracken)/
            centrifuge: meta.tool == 'centrifuge'
            kraken2: meta.tool == 'kraken2'
        }


    // Standardise and aggregate
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
        .map { meta, files ->
            [meta, file(files)]
        }

    KRAKENTOOLS_COMBINEKREPORTS_CENTRIFUGER(ch_profiles_for_centrifuger)
    ch_multiqc_files = ch_multiqc_files.mix(KRAKENTOOLS_COMBINEKREPORTS_CENTRIFUGER.out.txt)
    ch_versions = ch_versions.mix(KRAKENTOOLS_COMBINEKREPORTS_CENTRIFUGER.out.versions)

    PLOT_CENTRIFUGER(
        CENTRIFUGER_STANDARDISE.out.standardised_profile
            .filter { meta, _report ->
                meta.tool == 'centrifuge'
            },
        "Centrifuger, Taxpasta",
        Channel.value(file(params.tax_prof_gtdb_metadata, checkIfExists: true)),
        file("/mnt/workflow/definition/mag-v3.4.2/docs/images/mi_logo.png"),
        file(params.tax_prof_template, checkIfExists: true)
    )

    // Kraken2
    // Collect and replace id for db_name for prefix
    // Have to sort by size to ensure first file actually has hits otherwise
    // the script fails
    ch_profiles_for_kraken2 = groupProfiles(
        ch_input_profiles.kraken2,
        [sort: { -it.size() }],
        )
        .map { meta, files ->
            [meta, file(files)]
        }

    PLOT_KRAKEN2(
        KRAKEN2BRACKEN_STANDARDISE.out.standardised_profile
            .filter { meta, _report ->
                meta.tool == 'kraken2' || meta.tool == 'bracken'
            },
        "Kraken2, Taxpasta",
        k2_taxonomy_db,
        file("/mnt/workflow/definition/mag-v3.4.2/docs/images/mi_logo.png"),
        file(params.tax_prof_template, checkIfExists: true)
    )

    KRAKENTOOLS_COMBINEKREPORTS_KRAKEN(ch_profiles_for_kraken2)
    ch_multiqc_files = ch_multiqc_files.mix(KRAKENTOOLS_COMBINEKREPORTS_KRAKEN.out.txt)
    ch_versions = ch_versions.mix(KRAKENTOOLS_COMBINEKREPORTS_KRAKEN.out.versions)


    emit:
    ch_taxpasta_report   = ch_merge_reports
    ch_combined_bracken  = BRACKEN_COMBINEBRACKENOUTPUTS.out.txt
    ch_combined_kreports = KRAKENTOOLS_COMBINEKREPORTS_KRAKEN.out.txt.mix(KRAKENTOOLS_COMBINEKREPORTS_CENTRIFUGER.out.txt)
    versions             = ch_versions
    multiqc_files        = ch_multiqc_files
}
