process TAXPASTA_MERGE {
    tag "${meta.id}"
    label 'process_single'

    conda "${moduleDir}/environment.yml"
    container "${workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container
        ? 'https://depot.galaxyproject.org/singularity/taxpasta:0.7.0--pyhdfd78af_1'
        : '125434852769.dkr.ecr.us-east-1.amazonaws.com/quay/biocontainers/taxpasta:0.7.0--pyhdfd78af_1'}"

    input:
    tuple val(meta), path(profiles)
    val profiler
    val format
    path taxonomy

    output:
    tuple val(meta), path("*.{tsv,csv,arrow,parquet,biom}"), emit: merged_profiles
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    def taxonomy_option = taxonomy ? "--taxonomy ${taxonomy}" : ''
    def tax_profiles = profiles.collect { files -> "${files}"}.join(' ')
    """
    taxpasta merge \\
        --profiler ${profiler} \\
        --output ${prefix}.${format} \\
        ${args} \\
        ${taxonomy_option} \\
        ${tax_profiles}


    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        taxpasta: \$(taxpasta --version)
    END_VERSIONS
    """

    stub:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    def taxonomy_option = taxonomy ? "--taxonomy ${taxonomy}" : ''
    """
    touch ${prefix}.${format}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        taxpasta: \$(taxpasta --version)
    END_VERSIONS
    """
}
