process PLOT_INDVTAXHITS {
    tag "$meta.id"
    label 'process_single'

    conda "${moduleDir}/environment.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        '125434852769.dkr.ecr.us-east-1.amazonaws.com/quay/biocontainerslma_img:latest' :
        '125434852769.dkr.ecr.us-east-1.amazonaws.com/quay/biocontainerslma_img:latest' }"

    input:
    tuple val(meta), path(taxhits)
    val(mode)
    val(db)
    path(logo)
    path(template)

    output:
    tuple val(meta), path("*.html"), emit: report


    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    def taxp_kraken2 = mode.matches("Kraken2, Taxpasta") ? "--taxpasta_kraken2 $taxhits" : ""
    def taxp_centrifuger = mode.matches("Centrifuger, Taxpasta") ? "--taxpasta_centrifuger $taxhits" : ""
    def bracken_kraken2 = mode.matches("Kraken2, Bracken") ? "--bracken_kraken2 $taxhits" : ""
    def bracken_centrifuger = mode.matches("Centrifuger, Bracken") ? "--bracken_centrifuger $taxhits" : ""
    def sylph = mode.matches("Sylph") ? "--sylph $taxhits --syl_fn $db" : ""

    """
    chmod +x /mnt/workflow/definition/mag-v3.4.2/bin/plot_taxhits.py
    chmod +rwx /mnt/workflow/definition/mag-v3.4.2/data/gtdb_r220_metadata.tsv.gz

    python3 /mnt/workflow/definition/mag-v3.4.2/bin/plot_taxhits.py \\
       $args \\
       $taxp_kraken2 \\
       $taxp_centrifuger \\
       $bracken_kraken2 \\
       $bracken_centrifuger \\
       $sylph \\
       --sample_id ${meta.id} \\
       --logo $logo \\
       --report_template $template \\
       --output $prefix

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python --version 2>&1 | sed 's/Python //g')
        pandas: \$(python -c "import pkg_resources; print(pkg_resources.get_distribution('pandas').version)")
        seaborn: \$(python -c "import pkg_resources; print(pkg_resources.get_distribution('seaborn').version)")
    END_VERSIONS
    """
}
