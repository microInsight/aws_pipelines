//
// Run prodigal as orf caller then generate nice format for gff
//
include { PYRODIGAL as PYRODIGAL_MODULE } from '../../../modules/nf-core/pyrodigal/main'
include { PRODIGAL as PRODIGAL_MODULE   } from '../../../modules/nf-core/prodigal/main'
include { FORMAT_PRODIGAL_GFF           } from '../../../modules/local/format/prodigal/main'

workflow PRODIGAL {
    take:
    fastafile

    main:
    ch_versions = Channel.empty()
    ch_amino_acids = Channel.empty()
    ch_gff = Channel.empty()

    if (params.orf_caller == 'prodigal') {
        PRODIGAL_MODULE     ( fastafile, 'gff' )
        ch_versions = ch_versions.mix(PRODIGAL_MODULE.out.versions)

        FORMAT_PRODIGAL_GFF ( PRODIGAL_MODULE.out.gene_annotations )
        ch_versions = ch_versions.mix(FORMAT_PRODIGAL_GFF.out.versions)

        ch_amino_acids = PRODIGAL_MODULE.out.amino_acid_fasta
        ch_gff = PRODIGAL_MODULE.out.gene_annotations
    } else {
        PYRODIGAL_MODULE    ( fastafile, 'gff' )
        ch_versions = ch_versions.mix(PYRODIGAL_MODULE.out.versions)

        FORMAT_PRODIGAL_GFF ( PYRODIGAL_MODULE.out.annotations )
        ch_versions = ch_versions.mix(FORMAT_PRODIGAL_GFF.out.versions)

        ch_amino_acids = PYRODIGAL_MODULE.out.faa
        ch_gff = PYRODIGAL_MODULE.out.annotations
    }
    
    

    emit:
    faa     = ch_amino_acids
    gff     = ch_gff
    versions = ch_versions

}
