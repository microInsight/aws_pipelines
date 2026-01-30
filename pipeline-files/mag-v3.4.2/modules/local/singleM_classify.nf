process SINGLEM_CLASSIFY {
    tag "${meta.id}"
    label 'process_medium'

    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
    'https://depot.galaxyproject.org/singularity/singlem:0.18.3--pyhdfd78af_0' :
    'quay.io/biocontainers/singlem:0.20.3--pyhdfd78af_1' }"

    input:
    tuple val(meta), path(reads)
    path(metapackage)

    output:
    tuple val(meta), path("${meta.id}_profile.tsv"), path("${meta.id}_otu_table.csv"), emit: singleM_output
    tuple val(meta), path("*.tsv"), emit: singleM_profile
    tuple val(meta), path("*.csv"), emit: singleM_otu
    tuple val(meta), path(".html"), emit: singleM_krona
    path "versions.yml"           , emit: versions

    script:
    def args = task.ext.args ?: ''

    """
    singlem pipe \\
        -1 ${reads[0]} \\
        -2 ${reads[1]} \\
        -p ${meta.id}_profile.tsv \\
        --taxonomic-profile-krona ${meta.id}_profile_krona.html \\
        --otu-table ${meta.id}_otu_table.csv \\
        --threads ${task.cpus} \\
        --metapackage ${metapackage}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        singleM: \$(singlem pipe --version)
    END_VERSIONS
    """
}
