// Taxpasta merge and standardise profiling outputs

include { PLOT_INDVTAXHITS as PLOT_KRAKEN2                                       } from '../../modules/local/plot_indvtaxhits'
include { PLOT_INDVTAXHITS as PLOT_CENTRIFUGER                                   } from '../../modules/local/plot_indvtaxhits'
include { PLOT_INDVTAXHITS as PLOT_KRAKEN2BRACKEN                                } from '../../modules/local/plot_indvtaxhits'
include { PLOT_INDVTAXHITS as PLOT_CENTRIFUGERBRACKEN                            } from '../../modules/local/plot_indvtaxhits'
include { TAXPASTA_MERGE                                                         } from '../../modules/nf-core/taxpasta/merge/main'
include { TAXPASTA_STANDARDISE                                                   } from '../../modules/nf-core/taxpasta/standardise/main'
include { BRACKEN_COMBINEBRACKENOUTPUTS                                          } from '../../modules/nf-core/bracken/combinebrackenoutputs/main'
include { KRAKENTOOLS_COMBINEKREPORTS as KRAKENTOOLS_COMBINEKREPORTS_KRAKEN      } from '../../modules/nf-core/krakentools/combinekreports/main'
include { KRAKENTOOLS_COMBINEKREPORTS as KRAKENTOOLS_COMBINEKREPORTS_CENTRIFUGER } from '../../modules/nf-core/krakentools/combinekreports/main'

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
        .map { meta, profile -> [meta.tool, profile] }
        .groupTuple(groupTupleOptions)
        .map { tool, profiles -> [[id: tool], profiles] }
}

workflow TAXONOMIC_STANDARDISATION {
    take:
    profiles

    main:
    ch_versions = Channel.empty()
    ch_multiqc_files = Channel.empty()

    // Taxpasta standardisation
    ch_prepare_for_taxpasta = profiles
        .groupTuple()
        .map { meta, profile ->
            if (meta.tool =~ /(bracken)/){
                [meta + [tool: "bracken"], profile]
            }
            else if(meta.tool == /centrifuger/) {
                [meta + [tool: "centrifuge"], profile]
            }
            else {
                [meta, profile]
            }
        }

    // split GTDB R226 taxonomic information for taxpasta standardisation
    ch_taxpasta_tax_dir = params.taxpasta_taxonomy_dir ? file(params.taxpasta_taxonomy_dir, checkIfExists: true) : []

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
    ch_versions = ch_versions.mix(TAXPASTA_MERGE.out.versions)
    ch_versions = ch_versions.mix(TAXPASTA_STANDARDISE.out.versions)

    /*
        Split profile results based on tool they come from
    */
    ch_input_profiles = profiles
        .branch { meta, profile ->
            bracken: meta.tool =~ /(bracken)/
            centrifuge: meta.tool == 'centrifuge'
            kraken2: meta.tool == 'kraken2'
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

    PLOT_KRAKEN2(
        TAXPASTA_STANDARDISE.out.standardised_profile,
        "Kraken2, Taxpasta",
        Channel.value(file(params.tax_prof_gtdb_metadata, checkIfExists: true)),
        file("/mnt/workflow/definition/mag-v3.4.2/docs/images/mi_logo.png"),
        file(params.tax_prof_template, checkIfExists: true)
    )

    KRAKENTOOLS_COMBINEKREPORTS_KRAKEN(ch_profiles_for_kraken2)
    ch_multiqc_files = ch_multiqc_files.mix(KRAKENTOOLS_COMBINEKREPORTS_KRAKEN.out.txt)
    ch_versions = ch_versions.mix(KRAKENTOOLS_COMBINEKREPORTS_KRAKEN.out.versions)


    emit:
    ch_taxpasta_report = TAXPASTA_MERGE.out.merged_profiles
    versions = ch_versions
    multiqc_files = ch_multiqc_files
}
