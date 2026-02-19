process BAKTA_PLOT {
    tag "$meta.id"
    label 'process_medium'

    conda "${moduleDir}/environment.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/bakta:1.10.4--pyhdfd78af_0' :
        'biocontainers/bakta:1.10.4--pyhdfd78af_0' }"

    input:
    tuple val(meta), path(json)
    val assembly_or_bins

    output:
    tuple val(meta), path("${prefix}.${out_type}.{png,svg}") , emit: plot
    path "versions.yml"                                      , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args   ?: ''
    prefix   = task.ext.prefix ?: "${meta.id}"
    out_type = "${assembly_or_bins}" == "assembly" ? "genome" : "mag"
    
    """
    mkdir ./temp

    bakta_plot \\
        --type features \\
        $json \\
        $args \\
        --output "${params.outdir}/Annotation/Bakta/${meta.id}/Genome_Plot/Features/" \\
        --verbose

    bakta_plot \\
        --type cog \\
        $json \\
        $args \\
        --output "${params.outdir}/Annotation/Bakta/${meta.id}/Genome_Plot/COG/" \\
        --verbose
    
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        bakta: \$(echo \$(bakta_plot --version) 2>&1 | cut -f '2' -d ' ')
    END_VERSIONS
    """

    stub:
    prefix = task.ext.prefix ?: "${meta.id}"
    """
    touch ${prefix}.embl
    touch ${prefix}.faa
    touch ${prefix}.ffn
    touch ${prefix}.fna
    touch ${prefix}.gbff
    touch ${prefix}.gff3
    touch ${prefix}.hypotheticals.tsv
    touch ${prefix}.hypotheticals.faa
    touch ${prefix}.tsv
    touch ${prefix}.txt

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        bakta: \$(echo \$(bakta --version) 2>&1 | cut -f '2' -d ' ')
    END_VERSIONS
    """
}
