process SINGLEM_CLASSIFY {
    tag "${meta.id}"
    label 'process_medium'

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
    'https://depot.galaxyproject.org/singularity/singlem:0.18.3--pyhdfd78af_0' :
    'quay.io/biocontainers/singlem:0.20.3--pyhdfd78af_1' }"

    input:
    tuple val(meta), path(reads)
    path(metapackage)
    val(input_type)

    output:
    tuple val(meta), path("${meta.id}-${meta.bin}_profile.tsv"), path("${meta.id}-${meta.bin}_otu_table.csv"), emit: singleM_output
    tuple val(meta), path("*.tsv")                                                   , emit: singleM_profile
    tuple val(meta), path("*.csv")                                                   , emit: singleM_otu
    tuple val(meta), path("*.html")                                                  , emit: singleM_krona
    path "versions.yml"                                                              , emit: versions

    script:
    def args = task.ext.args ?: ''
    def read_args = params.single_end ? "-1 ${reads[0]}" : "-1 ${reads[0]} -2 ${reads[1]}"

    """
    if [ ${input_type} == "fasta" ]; then
        singlem pipe \\
            --genome-fasta-files ${reads} \\
            -p ${meta.id}-${meta.bin}_profile.tsv \\
            --taxonomic-profile-krona ${meta.id}-${meta.bin}_profile_krona.html \\
            --otu-table ${meta.id}_otu_table.csv \\
            --threads ${task.cpus} \\
            --metapackage ${metapackage}
    else
        singlem pipe \\
            ${read_args} \\
            -p ${meta.id}-${meta.bin}_profile.tsv \\
            --taxonomic-profile-krona ${meta.id}-${meta.bin}_profile_krona.html \\
            --otu-table ${meta.id}-${meta.bin}_otu_table.csv \\
            --threads ${task.cpus} \\
            --metapackage ${metapackage}
    fi

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        singleM: \$(singlem pipe --version)
    END_VERSIONS
    """
}
