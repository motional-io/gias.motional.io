<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\File;
use Illuminate\Support\Facades\Schema;
use Illuminate\Support\Facades\Storage;
use Illuminate\Support\Str;
use League\Csv\Writer;

class PopulateGiasData extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'gias:populate';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Command description';

    /**
     * Execute the console command.
     *
     * @return int
     */
    public function handle()
    {
        $today = now()->format('Ymd');
        $csvFile = "gias/edubasealldata{$today}-fixed.csv";

        if (Storage::missing($csvFile)) {
            $this->warn('CSV file missing - attempting to download...');
            $this->call('gias:download');
        }

        $this->dropDataTables();
        $this->dropHoldingTables();
        $this->createPostGis();
        $this->createTypes();

        $this->createHoldingTables();
        $this->populateHoldingTables($csvFile);
        $this->createDataTables();
        $this->populateDataTables();
        $this->dropHoldingTables();
        $this->importTrusts();
        $this->createViews();
        $this->refreshViews();


        return 0;
    }

    private function createPostGis(): void
    {
        $this->line('Adding PostGis Extension');
        DB::statement(File::get(base_path('gias-query-tool/ddl/extensions/postgis.sql')));
    }

    private function createTypes(): void
    {
        $this->line('Creating Types');
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/types/establishment.sql')));
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/types/establishment_group.sql')));
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/types/gender.sql')));
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/types/ofsted_rating.sql')));
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/types/phase.sql')));
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/types/rural_urban_classification.sql')));
    }

    private function createHoldingTables(): void
    {
        $this->line('Creating Holding Tables');
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/tables/create_schools_raw.sql')));
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/tables/create_email_addresses_raw.sql')));
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/tables/create_deprivation_pupil_premium_raw.sql')));
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/tables/geo/create_electoral_regions_raw.sql')));
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/tables/geo/create_local_authority_districts_raw.sql')));
    }

    private function populateHoldingTables(string $csvFile): void
    {
        $csvPath = Storage::path($csvFile);
        $emailPath = base_path('gias-query-tool/data/email-addresses-2020-09-01.csv');
        $ppPath = base_path('gias-query-tool/data/pupil-premium-2019-2020.csv');

        $this->line('Populating Holding Tables');
        DB::unprepared("copy schools_raw from '{$csvPath}' with csv header");
        DB::unprepared("copy email_addresses_raw from '{$emailPath}' with csv header");
        DB::unprepared("copy deprivation_pupil_premium_raw from '{$ppPath}' with csv header");
        $this->importRegionsRaw();
        $this->importDistrictsRaw();
    }

    private function createDataTables(): void
    {
        $this->line('Creating Data Tables');
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/tables/create_schools.sql')));
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/tables/create_deprivation_pupil_premium.sql')));
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/tables/geo/create_regions.sql')));
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/tables/geo/create_local_authorities.sql')));
    }

    private function populateDataTables(): void
    {
        $this->line('Populating Data Tables');
        DB::unprepared(File::get(base_path('gias-query-tool/dml/import_schools.sql')));
        DB::unprepared(File::get(base_path('gias-query-tool/dml/import_deprivation_pupil_premium.sql')));
        DB::unprepared(File::get(base_path('gias-query-tool/dml/geo/import_regions.sql')));
        DB::unprepared(File::get(base_path('gias-query-tool/dml/geo/import_districts.sql')));

    }

    private function dropHoldingTables(): void
    {
        $this->line('Dropping Holding Tables');
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/tables/drop_schools_raw.sql')));
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/tables/drop_email_addresses_raw.sql')));
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/tables/drop_deprivation_pupil_premium_raw.sql')));
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/tables/geo/drop_electoral_regions_raw.sql')));
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/tables/geo/drop_local_authority_districts_raw.sql')));
    }

    private function createViews(): void
    {
        $this->line('Creating Views');
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/views/open_schools.sql')));
    }

    private function refreshViews(): void
    {
        $this->line('Refreshing Views');
        DB::unprepared(File::get(base_path('gias-query-tool/ddl/refresh/refresh_open_schools.sql')));
    }

    private function dropDataTables(): void
    {
        $this->line('Dropping Data Tables');
        DB::statement('drop table if exists schools cascade');
        DB::statement('drop table if exists deprivation_pupil_premium cascade');
        DB::statement('drop table if exists local_authorities cascade');
        DB::statement('drop table if exists regions cascade');
        DB::statement('drop table if exists trusts cascade ');
    }

    private function importDistrictsRaw(): void
    {
        $path = 'gias-query-tool/dml/geo/import_local_authority_districts.sql';
        $newPath = 'gias/districts.csv';
        $header = [
            'ogc_fid',
            'id',
            'lad13cd',
            'lad13cdo',
            'lad13nm',
            'lad13nmw',
            'wkb_geometry'
        ];

        $this->processStdin($path, $newPath, $header);

    }

    private function importRegionsRaw(): void
    {
        $path = 'gias-query-tool/dml/geo/import_electoral_regions.sql';
        $newPath = 'gias/regions.csv';
        $header = [
            'ogc_fid',
            'eer13cd',
            'eer13cdo',
            'eer13nm',
            'wkb_geometry'
        ];

        $this->processStdin($path, $newPath, $header);

    }

    private function processStdin($path, $newPath, $header): void
    {
        $csvFullPath = Storage::path($newPath);
        $lines = Str::of(File::get($path))
            ->split('/\r\n|\n|\r/');
        $command = Str::of($lines->shift())
            ->replace('stdin;', "'{$csvFullPath}' with csv header")
            ->value();

        $data = $lines->map(function ($line) {
            return Str::of($line)
                ->replace('\N', 'NULL')
                ->split('/\t/')
                ->toArray();
        })
            ->filter(function ($line) {
                info(collect($line)->count());
                return collect($line)->count() >= 5;
            })
            ->prepend($header)
            ->toArray();


        $content = Writer::createFromString();

        $content->insertAll($data);

        Storage::put($newPath, $content);

        DB::unprepared($command);
    }

    private function importTrusts(): void
    {
        Schema::create('trusts', function (Blueprint $table) {
            $table->integer('code');
            $table->string('name');
        });

        DB::table('schools')
            ->select('trust_code', 'trust_name')
            ->whereNotNull('trust_code')
            ->get()
            ->filter()
            ->unique()
            ->each(fn($value) => DB::insert('insert into trusts (code, name) values (?,?)', [
                $value->trust_code,
                $value->trust_name
            ]));
    }


}
