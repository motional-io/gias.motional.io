<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;

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
    protected $description = 'Downloads a CSV file and adds it into';

    /**
     * Execute the console command.
     *
     * @return int
     */
    public function handle()
    {
        return 0;
    }
}
