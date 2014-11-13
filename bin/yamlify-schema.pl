# Convert KE EMu schema.pl to python readable YAML
# Call with: perl yamlify-schema.pl schema_file output_dir

use YAML;
use File::Basename;

#! /usr/local/bin/perl -w

# (1) quit unless we have the correct number of command-line args
$num_args = $#ARGV + 1;
if ($num_args != 2) {
    print "\nUsage: yamlify-schema.pl schema_file output_dir\n";
    exit;
}

# (2) we got two command line args, so assume they are the
# first name and last name
$schema_file = $ARGV[0];
$output_dir = $ARGV[1];

my $output_file = $output_dir . '/schema.yaml';

require "$schema_file";

open F, '>', "$output_file";
print F Dump(%Schema);
close F;

print 0