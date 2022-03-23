<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Storage;

class DownloadGiasData extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'gias:download';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Downloads a CSV file';

    /**
     * Execute the console command.
     *
     * @return int
     */
    public function handle()
    {
        $this->line('Deleting existing files');
        Storage::deleteDirectory('gias');

        $today = now()->format('Ymd');
        $this->info("Date set to {$today}");

        $giasFilename = "edubasealldata{$today}.csv";
        $fixedFilename = "gias/edubasealldata{$today}-fixed.csv";
        $giasPath = "https://ea-edubase-api-prod.azurewebsites.net/edubase/downloads/public/${giasFilename}";

        $this->line('Attempting to fetch CSV...');
        $response = Http::timeout(30)
            ->retry(3, 1000)
            ->get($giasPath);

        if ($response->clientError()) {
            $this->error('Failed due to a client error');
            return 1;
        }

        if ($response->serverError()) {
            $this->error('Failed due to a server error');
            return 1;
        }

        Storage::put($fixedFilename, iconv('ISO-8859-1', 'UTF-8', $response->body()));

        if (Storage::missing($fixedFilename)) {
            $this->error('Download Failed');
            return 1;
        }

        $this->info('CSV downloaded successfully');
        return 0;
    }
}
